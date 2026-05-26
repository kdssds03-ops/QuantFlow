"""
worker.tasks — QuantFlow 하이브리드 매매 및 실시간 텔레그램 알림 연동 엔진

Phase 3: 실전 거래소 주문 집행 레이어 고도화
  - Exponential Backoff Retry (tenacity): NetworkError/RequestTimeout 최대 3회 재시도
  - Strict Error Catching: InsufficientFunds/InvalidOrder 즉시 REJECTED 처리
  - 주문 체결 사후 정산 폴링: open 상태 주문 최대 5초 추적 → 미체결 강제 취소
  - 텔레그램 연동 캡슐화: 상태별(FILLED/PARTIALLY_FILLED/REJECTED) 차별화 알림

Phase 5 (Timezone & Warmup Fix):
  - UTC 타임존 강제 정규화: 모든 타임스탬프를 timezone-aware UTC로 통일
  - DB Warm-up: 봇 재시작 시 DB 과거 500봉으로 지표 재계산 → NULL 레코드 일괄 upsert
  - OHLCV 수집 limit 500봉으로 확대 → EMA50/SMA20 warm-up NaN 원천 차단
"""

import logging
import math
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import TypedDict

import ccxt
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from worker.celery_app import celery_app
from core.config import get_settings
from core.exchange import get_exchange
from core.time_sync import check_ntp_drift
from app.models.models import MarketData, TradeHistory

# [관심사 분리] 실시간 텔레그램 알림 모듈 결합
from core.notifier import notifier

logger = logging.getLogger(__name__)
settings = get_settings()

# ── 동기 DB 엔진 구성 ──────────────────────────────────────────────────
_sync_engine = create_engine(
    settings.sync_database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
_SyncSession = sessionmaker(bind=_sync_engine, expire_on_commit=False)


def _get_sync_session():
    """동기 DB 세션 컨텍스트 매니저"""
    session = _SyncSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# 🚨 [치명적 버그 픽스]: 워커 재부팅 시 데이터를 통째로 지워버리던 drop_all 무력화
# def _ensure_table_exists():
#     from core.database import Base
#     import app.models.models
#     Base.metadata.drop_all(_sync_engine)  # <-- 기존 데이터 증발의 원인
#     Base.metadata.create_all(_sync_engine)
# _ensure_table_exists()


# ── Predictor 동적 의존성 주입 (Dependency Injection) 및 싱글턴 초기화 ──
import os
from worker.predictor import BasePredictor, RuleBasedPredictor, MLPredictor

def _resolve_predictor() -> BasePredictor:
    """
    환경 변수 'PREDICTOR_TYPE' 설정값에 따라 다형성이 보장된 적절한 매매 예측 엔진을 동적으로 주입합니다.
    """
    predictor_type = os.getenv("PREDICTOR_TYPE", "RULE").strip().upper()
    
    if predictor_type == "ML":
        logger.info("🤖 [Dependency Injection] 'ML' 예측 엔진 감지 -> MLPredictor 동적 주입 완료")
        return MLPredictor(session_factory=_SyncSession, confidence_threshold=CONFIDENCE_THRESHOLD)
    
    # "RULE" 이거나 설정되지 않은(None, 공백) 경우 RuleBasedPredictor로 안전하게 폴백
    logger.info("🟢 [Dependency Injection] 'RULE' 예측 엔진 감지 (또는 Fallback) -> RuleBasedPredictor 동적 주입 완료")
    return RuleBasedPredictor()

# Celery 워커 최초 메모리 가동 시 1회 싱글턴 초기화 수행
_predictor: BasePredictor = _resolve_predictor()

# ── [웰컴 알림] 파일시스템 영속성 플래그 기반 멱등성 가드 ────────────────
# Celery prefork 워커는 서브프로세스 분기 시 모듈을 재임포트할 수 있어
# Python 임포트 캐싱만으로는 중복 발송을 막을 수 없음.
# → 로컬 파일시스템에 .welcome_sent 플래그 파일이 존재하는지를 영속성 체크 기준으로 사용.
# → 발송 성공 후에만 플래그를 생성하여, 발송 실패 시 다음 기동 때 자동 재시도되도록 설계.
_WELCOME_FLAG_FILE = ".welcome_sent"

if not os.path.exists(_WELCOME_FLAG_FILE):
    try:
        from core.notifier import send_telegram_message
        _predictor_name = type(_predictor).__name__
        send_telegram_message(
            "🚀 <b>[QuantFlow] 매매 엔진 가동 완료!</b>\n"
            "스마트폰 관제 시스템이 백엔드 인프라와 성공적으로 유기적 연동되었습니다.\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>예측 엔진:</b> <code>{_predictor_name}</code>\n"
            f"• <b>환경 변수 PREDICTOR_TYPE:</b> <code>{os.getenv('PREDICTOR_TYPE', 'RULE (기본값)')}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        # 발송 성공 시에만 플래그 파일 생성 → 이후 기동부터는 이 블록 진입 자체를 차단
        with open(_WELCOME_FLAG_FILE, "w") as _f:
            from datetime import datetime as _dt
            _f.write(f"sent at {_dt.now().isoformat()}")
        logger.info("🚩 [웰컴 알림] 발송 완료 — 플래그 파일 생성됨: %s", _WELCOME_FLAG_FILE)
    except Exception as _e:
        # 발송 실패 시 플래그를 생성하지 않음 → 다음 워커 재기동 시 자동 재시도
        logger.error("❌ [웰컴 알림] 발송 실패 (플래그 미생성, 다음 기동 시 재시도): %s", _e)
else:
    logger.debug("🚩 [웰컴 알림] 플래그 파일 감지 — 중복 발송 차단: %s", _WELCOME_FLAG_FILE)

# ── [Strict Rules] 정밀도 유지 및 리스크 관리 임계치 설정 ────────────────
TRADE_AMOUNT_BTC = Decimal("0.001")     # 일반 float(0.001)에서 Decimal 구조로 정형화
MIN_ORDER_BTC = Decimal("0.0001")       # 바이낸스 등 거래소 시장가 주문 최저 한계선 (BTC)
COOLDOWN_MINUTES = 5
STOP_LOSS_THRESHOLD = Decimal("-0.0100") # -1.0% 소프트 손절 방패 (지표 기반)
CONFIDENCE_THRESHOLD = 0.65             # 스나이퍼 진입 확신도 65% Filter

# ── [8차 확장] 하드 TP/SL + 타임아웃 안전장치 임계치 ────────────────────
HARD_SL_THRESHOLD   = Decimal("-0.0150") # -1.5% 하드 손절 컷 (무조건 강제 청산)
HARD_TP_THRESHOLD   = Decimal("0.0300")  # +3.0% 하드 익절 타겟 (무조건 강제 수확)
MAX_POSITION_MINUTES = 180               # 최대 포지션 보유 시간 (180분 = 3시간)

# ── [Phase 3] 주문 집행 파이프라인 설정 상수 ─────────────────────────────
_ORDER_FILL_POLL_INTERVAL_SEC = 1   # 체결 폴링 간격 (초)
_ORDER_FILL_POLL_MAX_SEC = 5        # 체결 폴링 최대 대기 시간 (초)
_ORDER_RETRY_MAX_ATTEMPTS = 3       # 네트워크 재시도 최대 횟수
_ORDER_RETRY_WAIT_MIN_SEC = 1       # Exponential Backoff 최소 대기 (초)
_ORDER_RETRY_WAIT_MAX_SEC = 8       # Exponential Backoff 최대 대기 (초)
_BALANCE_RETRY_MAX = 3              # fetch_balance 재시도 최대 횟수
_BALANCE_RETRY_WAIT_SEC = 1         # fetch_balance 재시도 대기 (초)


# ─────────────────────────────────────────────
# 🛡️ [Phase 3] 주문 집행 파이프라인 결과 타입
# ─────────────────────────────────────────────
class OrderResult(TypedDict):
    """
    _execute_order_pipeline() 반환 타입 정의.
    매매 집행 결과를 모든 호출부에서 동일한 구조로 처리하기 위한 표준 인터페이스.
    """
    status: str          # "FILLED" | "PARTIALLY_FILLED" | "CANCELLED" | "REJECTED" | "FAILED"
    order_id: str        # 거래소 주문 번호 (실패 시 "unknown")
    filled_price: Decimal  # 평균 체결 단가 (미체결 시 입력 시세 기준)
    filled_amount: Decimal # 실제 체결 수량 (부분 체결 포함)
    reject_reason: str   # 거부/실패 원인 설명 (정상 체결 시 빈 문자열)


# ─────────────────────────────────────────────
# 🚀 [Phase 3] 주문 집행 파이프라인 — 프로덕션 등급 핵심 엔진
# ─────────────────────────────────────────────
def _execute_order_pipeline(
    exchange: ccxt.Exchange,
    symbol: str,
    side: str,
    amount: Decimal,
    trigger_type: str,
    fallback_price: Decimal,
    confidence: float,
    usdt_balance: Decimal,
) -> OrderResult:
    """
    금융권 프로덕션 등급 주문 집행 파이프라인.

    [STEP 1] Exponential Backoff Retry:
        NetworkError / RequestTimeout 발생 시 최대 3회 재시도 (1s → 2s → 4s).
        InsufficientFunds / InvalidOrder 는 즉시 REJECTED Short-circuit.

    [STEP 2] 주문 직후 상태 즉시 확인:
        status가 'closed'/'filled' → 즉시 FILLED 반환.
        status가 'open' → STEP 3 진입.

    [STEP 3] 체결 폴링 루프 (최대 5초, 1초 간격):
        fetch_order() 로 실시간 체결 상태 추적.
        타임아웃 초과 시 STEP 4 진입.

    [STEP 4] 미체결 잔량 강제 취소:
        cancel_order() 호출 후 실제 체결된 수량만 정산.
        filled_qty > 0 → PARTIALLY_FILLED / filled_qty == 0 → CANCELLED.

    모든 상태에서 텔레그램 알림 발송 후 OrderResult 반환.
    이 함수는 절대 예외를 상위로 전파하지 않으며, 최악의 경우 FAILED 반환.
    """
    _order_id: str = "unknown"
    _filled_price: Decimal = fallback_price
    _filled_amount: Decimal = Decimal("0")

    # ── STEP 1: Exponential Backoff Retry 가드 ──────────────────────────────
    # tenacity를 인라인 클로저로 감싸 재시도 로직을 캡슐화.
    # InsufficientFunds / InvalidOrder는 즉시 re-raise → retry 없이 탈출.
    def _create_order_with_retry() -> dict:
        """
        create_order를 Exponential Backoff로 감싸는 내부 실행기.
        일시적 네트워크 오류 시에만 재시도하며, 하드웨어/자산 한계 오류는
        즉시 상위로 전파하여 retry 루프를 즉각 탈출(Short-circuit)시킨다.
        """
        last_exc: Exception | None = None

        for attempt in range(1, _ORDER_RETRY_MAX_ATTEMPTS + 1):
            try:
                logger.info(
                    "📡 [%s] create_order 시도 %d/%d — %s %s BTC",
                    trigger_type, attempt, _ORDER_RETRY_MAX_ATTEMPTS, side, float(amount)
                )
                return exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=side.lower(),
                    amount=float(amount),
                )
            except (ccxt.InsufficientFunds, ccxt.InvalidOrder):
                # 하드웨어/자산 한계 — 재시도 불가, 즉시 상위로 전파
                raise
            except (ccxt.NetworkError, ccxt.RequestTimeout) as exc:
                last_exc = exc
                if attempt < _ORDER_RETRY_MAX_ATTEMPTS:
                    wait_sec = _ORDER_RETRY_WAIT_MIN_SEC * (2 ** (attempt - 1))  # 1s, 2s, 4s
                    logger.warning(
                        "⚠️ [%s] 네트워크 오류 (attempt %d/%d) — %.0fs 후 재시도: %s",
                        trigger_type, attempt, _ORDER_RETRY_MAX_ATTEMPTS, wait_sec, exc
                    )
                    time.sleep(wait_sec)
                else:
                    logger.error(
                        "❌ [%s] 네트워크 오류 최대 재시도(%d회) 초과 — 집행 포기: %s",
                        trigger_type, _ORDER_RETRY_MAX_ATTEMPTS, exc
                    )
            except ccxt.BaseError as exc:
                last_exc = exc
                logger.error("❌ [%s] CCXT 거래소 오류 — 재시도 없이 중단: %s", trigger_type, exc)
                break
            except Exception as exc:
                last_exc = exc
                logger.error("❌ [%s] 예상치 못한 시스템 오류 — 재시도 없이 중단: %s", trigger_type, exc)
                break

        raise last_exc or RuntimeError("create_order 알 수 없는 실패")

    # ── STEP 1 실행: 자산/수량 한계 예외 Strict Catching ──────────────────
    try:
        order = _create_order_with_retry()
    except ccxt.InsufficientFunds as exc:
        reject_reason = f"잔고 부족 (InsufficientFunds): {exc}"
        logger.error("🚨 [%s] %s", trigger_type, reject_reason)
        # 텔레그램 🚨 긴급 경고 발송
        notifier.send_message(
            f"🚨 <b>[QuantFlow] 주문 거부 — 잔고 부족</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>트리거:</b> <code>{trigger_type}</code>\n"
            f"• <b>심볼:</b> <code>{symbol}</code>\n"
            f"• <b>방향:</b> <code>{side.upper()}</code>\n"
            f"• <b>요청 수량:</b> <code>{float(amount):.4f} BTC</code>\n"
            f"• <b>가용 USDT:</b> <code>${float(usdt_balance):,.2f}</code>\n"
            f"• <b>원인:</b> <code>InsufficientFunds</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        return OrderResult(
            status="REJECTED", order_id="unknown",
            filled_price=fallback_price, filled_amount=Decimal("0"),
            reject_reason=reject_reason,
        )
    except ccxt.InvalidOrder as exc:
        reject_reason = f"유효하지 않은 주문 파라미터/수량 (InvalidOrder): {exc}"
        logger.error("🚨 [%s] %s", trigger_type, reject_reason)
        notifier.send_message(
            f"🚨 <b>[QuantFlow] 주문 거부 — 최소 수량 미달</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>트리거:</b> <code>{trigger_type}</code>\n"
            f"• <b>심볼:</b> <code>{symbol}</code>\n"
            f"• <b>방향:</b> <code>{side.upper()}</code>\n"
            f"• <b>요청 수량:</b> <code>{float(amount):.4f} BTC</code>\n"
            f"• <b>원인:</b> <code>InvalidOrder (최소 단위 미달)</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        return OrderResult(
            status="REJECTED", order_id="unknown",
            filled_price=fallback_price, filled_amount=Decimal("0"),
            reject_reason=reject_reason,
        )
    except Exception as exc:
        # 재시도 소진 또는 기타 예외
        fail_reason = f"주문 집행 최종 실패: {exc}"
        logger.error("❌ [%s] %s", trigger_type, fail_reason)
        return OrderResult(
            status="FAILED", order_id="unknown",
            filled_price=fallback_price, filled_amount=Decimal("0"),
            reject_reason=fail_reason,
        )

    # ── STEP 2: 주문 직후 상태 즉시 확인 ──────────────────────────────────
    _order_id = str(order.get("id", "unknown"))
    raw_status = str(order.get("status", "")).lower()
    _filled_amount = Decimal(str(order.get("filled") or 0))
    _filled_price = Decimal(str(
        order.get("average") or order.get("price") or fallback_price
    ))

    logger.info(
        "📋 [%s] create_order 응답 — id=%s, status=%s, filled=%s",
        trigger_type, _order_id, raw_status, _filled_amount
    )

    if raw_status in ("closed", "filled"):
        # 즉시 완전 체결 → 정산 후 알림 발송
        logger.info("✅ [%s] 즉시 완전 체결 확인: %s BTC @ $%s", trigger_type, _filled_amount, _filled_price)
        notifier.notify_trade(
            trigger_type=trigger_type, symbol=symbol, side=side,
            price=_filled_price, amount=_filled_amount, order_id=_order_id,
            confidence=confidence, balance=usdt_balance,
        )
        return OrderResult(
            status="FILLED", order_id=_order_id,
            filled_price=_filled_price, filled_amount=_filled_amount,
            reject_reason="",
        )

    # ── STEP 3: open 상태 → 체결 폴링 루프 ───────────────────────────────
    if raw_status == "open":
        logger.info(
            "⏳ [%s] 주문 미체결(open) 상태 감지 — 최대 %d초 폴링 시작 (order_id=%s)",
            trigger_type, _ORDER_FILL_POLL_MAX_SEC, _order_id
        )
        poll_filled = False
        for poll_no in range(1, _ORDER_FILL_POLL_MAX_SEC + 1):
            time.sleep(_ORDER_FILL_POLL_INTERVAL_SEC)
            try:
                fetched = exchange.fetch_order(_order_id, symbol)
                f_status = str(fetched.get("status", "")).lower()
                f_filled = Decimal(str(fetched.get("filled") or 0))
                f_price  = Decimal(str(
                    fetched.get("average") or fetched.get("price") or fallback_price
                ))
                logger.info(
                    "🔍 [%s] 폴링 %d/%d — status=%s, filled=%s BTC",
                    trigger_type, poll_no, _ORDER_FILL_POLL_MAX_SEC, f_status, f_filled
                )
                if f_status in ("closed", "filled"):
                    _filled_amount = f_filled
                    _filled_price  = f_price
                    poll_filled = True
                    break
            except ccxt.BaseError as exc:
                logger.warning(
                    "⚠️ [%s] fetch_order 폴링 오류 (무시 후 계속): %s", trigger_type, exc
                )

        if poll_filled:
            logger.info(
                "✅ [%s] 폴링 체결 확인: %s BTC @ $%s",
                trigger_type, _filled_amount, _filled_price
            )
            notifier.notify_trade(
                trigger_type=trigger_type, symbol=symbol, side=side,
                price=_filled_price, amount=_filled_amount, order_id=_order_id,
                confidence=confidence, balance=usdt_balance,
            )
            return OrderResult(
                status="FILLED", order_id=_order_id,
                filled_price=_filled_price, filled_amount=_filled_amount,
                reject_reason="",
            )

        # ── STEP 4: 폴링 타임아웃 → 미체결 잔량 강제 취소 ─────────────────
        logger.warning(
            "⏰ [%s] 폴링 %d초 초과 — 미체결 주문 강제 취소 시도 (order_id=%s)",
            trigger_type, _ORDER_FILL_POLL_MAX_SEC, _order_id
        )
        try:
            cancelled = exchange.cancel_order(_order_id, symbol)
            _filled_amount = Decimal(str(cancelled.get("filled") or 0))
            _filled_price  = Decimal(str(
                cancelled.get("average") or cancelled.get("price") or fallback_price
            ))
            logger.info(
                "🚫 [%s] cancel_order 완료 — 실 체결 수량: %s BTC",
                trigger_type, _filled_amount
            )
        except ccxt.BaseError as exc:
            logger.error(
                "❌ [%s] cancel_order 실패 (수동 확인 필요!): %s", trigger_type, exc
            )
            # cancel 실패 시에도 파이프라인은 FAILED 반환으로 안전 종료
            return OrderResult(
                status="FAILED", order_id=_order_id,
                filled_price=_filled_price, filled_amount=_filled_amount,
                reject_reason=f"cancel_order 실패: {exc}",
            )

        if _filled_amount > Decimal("0"):
            final_status = "PARTIALLY_FILLED"
            logger.info(
                "🟡 [%s] 부분 체결 후 취소 확정: %s BTC @ $%s",
                trigger_type, _filled_amount, _filled_price
            )
            # 부분 체결 알림 (성공 알림과 동일 채널 사용)
            notifier.notify_trade(
                trigger_type=f"{trigger_type} (PARTIALLY_FILLED)",
                symbol=symbol, side=side,
                price=_filled_price, amount=_filled_amount, order_id=_order_id,
                confidence=confidence, balance=usdt_balance,
            )
        else:
            final_status = "CANCELLED"
            logger.info("⚪ [%s] 미체결 전량 취소 완료 (CANCELLED)", trigger_type)
            notifier.send_message(
                f"⚪ <b>[QuantFlow] 주문 전량 미체결 취소</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>트리거:</b> <code>{trigger_type}</code>\n"
                f"• <b>심볼:</b> <code>{symbol}</code>\n"
                f"• <b>방향:</b> <code>{side.upper()}</code>\n"
                f"• <b>주문 번호:</b> <code>{_order_id}</code>\n"
                f"• <b>사유:</b> <code>{_ORDER_FILL_POLL_MAX_SEC}초 내 미체결 → 강제 취소</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )

        return OrderResult(
            status=final_status, order_id=_order_id,
            filled_price=_filled_price, filled_amount=_filled_amount,
            reject_reason="",
        )

    # ── 그 외 알 수 없는 상태 ─────────────────────────────────────────────
    unknown_reason = f"거래소 반환 알 수 없는 status: '{raw_status}'"
    logger.error("❌ [%s] %s", trigger_type, unknown_reason)
    return OrderResult(
        status="FAILED", order_id=_order_id,
        filled_price=_filled_price, filled_amount=_filled_amount,
        reject_reason=unknown_reason,
    )


# ─────────────────────────────────────────────
# 🔧 UTC 타임스탬프 정규화 헬퍼
# ─────────────────────────────────────────────
def _ensure_utc(ts_ms: int) -> datetime:
    """
    거래소 반환 밀리초 Unix 타임스탬프(항상 UTC)를 timezone-aware UTC datetime으로 변환.

    ── 타임존 버그 방어 전략 ──────────────────────────────────────────────────
    Binance (including demo-fapi) OHLCV timestamp는 항상 UTC milliseconds.
    Python의 datetime.fromtimestamp()는 로컬 시스템 시간대(KST)를 기준으로 변환하므로
    반드시 tz=timezone.utc 를 명시하여 UTC를 강제해야 합니다.
    """
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)


def _to_dec(val) -> Decimal | None:
    """float/Decimal NaN 및 None을 안전하게 None으로 변환 후 Decimal 반환."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    try:
        d = Decimal(str(val))
        if d.is_nan():
            return None
        return d
    except Exception:
        return None


# ─────────────────────────────────────────────
# 🔁 [8차 확장] 거래소 잔고 조회 재시도 헬퍼
# ─────────────────────────────────────────────
def _fetch_balance_with_retry(exchange: "ccxt.Exchange") -> dict:
    """
    fetch_balance()를 최대 _BALANCE_RETRY_MAX 회 자동 재시도.

    순간적인 네트워크 끊김이나 거래소 서버 점검으로 인해 잔고 조회가 실패할 때
    봇 전체가 셧다운되지 않도록 1초 간격 재시도로 보호합니다.
    모든 시도가 소진되면 마지막 예외를 re-raise합니다.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _BALANCE_RETRY_MAX + 1):
        try:
            return exchange.fetch_balance({'type': 'future'})
        except (ccxt.NetworkError, ccxt.RequestTimeout) as exc:
            last_exc = exc
            if attempt < _BALANCE_RETRY_MAX:
                logger.warning(
                    "⚠️ [잔고 조회] 네트워크 오류 (attempt %d/%d) — %ds 후 재시도: %s",
                    attempt, _BALANCE_RETRY_MAX, _BALANCE_RETRY_WAIT_SEC, exc,
                )
                time.sleep(_BALANCE_RETRY_WAIT_SEC)
        except Exception as exc:
            last_exc = exc
            logger.error("❌ [잔고 조회] 복구 불가 예외 — 재시도 없이 중단: %s", exc)
            break
    raise last_exc or RuntimeError("fetch_balance 알 수 없는 실패")


# ─────────────────────────────────────────────
# 🔄 DB Warm-up: 재시작 시 과거 데이터로 지표 재계산
# ─────────────────────────────────────────────
def _warmup_from_db(symbol: str = "BTC/USDT", lookback: int = 500) -> int:
    """
    봇 재시작 시 DB에 저장된 최근 lookback 개 캔들을 읽어
    compute_all_features()로 지표를 재계산하고, 지표값이 NULL인 레코드를 일괄 upsert.

    ── 해결하는 문제 ──────────────────────────────────────────────────────────
    봇 최초 기동 또는 재시작 직후에 메모리 큐가 비어 있어 발생하는 초기 NaN 루프를
    DB에 이미 누적된 과거 데이터를 활용하여 즉시 해소합니다.

    Returns:
        upsert된 레코드 수
    """
    logger.info(
        "🔄 [DB Warm-up] 시작 — symbol=%s, lookback=%d봉", symbol, lookback
    )
    try:
        session = _SyncSession()
        try:
            from sqlalchemy import asc
            rows = (
                session.query(MarketData)
                .filter(MarketData.symbol == symbol)
                .order_by(desc(MarketData.timestamp))
                .limit(lookback)
                .all()
            )
        finally:
            session.close()

        if not rows:
            logger.warning("🔄 [DB Warm-up] DB에 저장된 데이터 없음 — Warm-up 스킵")
            return 0

        # 시간 오름차순 정렬 (oldest → newest)
        rows.reverse()

        # ── 타임존 방어 코드: DB 레코드의 timestamp를 UTC-aware로 정규화 ──────
        # PostgreSQL DateTime(timezone=True) 컬럼은 timezone-aware를 반환하지만,
        # 혹시 naive datetime이 섞여 있을 경우를 대비하여 강제 정규화
        def _normalize_ts(ts: datetime) -> datetime:
            if ts is None:
                return ts
            if ts.tzinfo is None:
                # naive datetime → UTC로 가정하고 tz 부여
                return ts.replace(tzinfo=timezone.utc)
            # timezone-aware이면 UTC로 변환
            return ts.astimezone(timezone.utc)

        # DataFrame 재구성 (OHLCV만 — 지표는 재계산)
        df_rows = []
        for r in rows:
            df_rows.append({
                "timestamp_ms": int(_normalize_ts(r.timestamp).timestamp() * 1000),
                "open":   float(r.open),
                "high":   float(r.high),
                "low":    float(r.low),
                "close":  float(r.close),
                "volume": float(r.volume),
            })

        df = pd.DataFrame(df_rows)

        # ── compute_all_features: 연속 시계열 정렬 검증 ──────────────────────
        # timestamp_ms 컬럼으로 정렬하여 시간 불연속(타임존 혼재로 인한 순서 역전) 방지
        df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

        from worker.indicators import compute_all_features
        df = compute_all_features(df)

        import ta
        if len(df) < 26:
            logger.warning(f"⚠️ [DB Warm-up] 데이터 캔들 수가 부족하여 지표 계산을 스킵합니다. (현재: {len(df)}봉, 필요: 최소 26봉)")
        else:
            try:
                df['atr_14'] = ta.volatility.average_true_range(high=df['high'], low=df['low'], close=df['close'], window=14)
                df['macd_line'] = ta.trend.macd(close=df['close'], window_fast=12, window_slow=26)
                df['macd_signal'] = ta.trend.macd_signal(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
                df['macd_hist'] = ta.trend.macd_diff(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
            except IndexError as e:
                logger.warning(f"⚠️ [DB Warm-up] 지표 계산 중 IndexError 발생 (데이터 부족): {e}")
            except Exception as e:
                logger.warning(f"⚠️ [DB Warm-up] 지표 계산 중 예외 발생: {e}")

        # ── NULL 지표 레코드만 선별하여 upsert ──────────────────────────────
        upsert_count = 0
        for i, r in enumerate(rows):
            row_feat = df.iloc[i]
            sma_20_val   = _to_dec(row_feat.get("sma_20"))
            rsi_14_val   = _to_dec(row_feat.get("rsi_14"))
            bb_upper_val = _to_dec(row_feat.get("bb_upper"))
            bb_lower_val = _to_dec(row_feat.get("bb_lower"))
            atr_14_val      = _to_dec(row_feat.get("atr_14"))
            macd_line_val   = _to_dec(row_feat.get("macd_line"))
            macd_signal_val = _to_dec(row_feat.get("macd_signal"))
            macd_hist_val   = _to_dec(row_feat.get("macd_hist"))

            # 지표가 NULL인 행만 upsert (정상 행 불필요한 재기록 방지)
            if r.rsi_14 is None or r.bb_upper is None or r.bb_lower is None or getattr(r, 'atr_14', None) is None:
                ts_utc = _normalize_ts(r.timestamp)
                stmt = pg_insert(MarketData).values(
                    timestamp=ts_utc,
                    symbol=symbol,
                    open=Decimal(str(r.open)),
                    high=Decimal(str(r.high)),
                    low=Decimal(str(r.low)),
                    close=Decimal(str(r.close)),
                    volume=Decimal(str(r.volume)),
                    sma_20=sma_20_val,
                    rsi_14=rsi_14_val,
                    bb_upper=bb_upper_val,
                    bb_lower=bb_lower_val,
                    atr_14=atr_14_val,
                    macd_line=macd_line_val,
                    macd_signal=macd_signal_val,
                    macd_hist=macd_hist_val,
                ).on_conflict_do_update(
                    constraint="uq_market_data_ts_symbol",
                    set_={
                        "sma_20":   pg_insert(MarketData).excluded.sma_20,
                        "rsi_14":   pg_insert(MarketData).excluded.rsi_14,
                        "bb_upper": pg_insert(MarketData).excluded.bb_upper,
                        "bb_lower": pg_insert(MarketData).excluded.bb_lower,
                        "atr_14":   pg_insert(MarketData).excluded.atr_14,
                        "macd_line": pg_insert(MarketData).excluded.macd_line,
                        "macd_signal": pg_insert(MarketData).excluded.macd_signal,
                        "macd_hist": pg_insert(MarketData).excluded.macd_hist,
                    },
                )
                for session in _get_sync_session():
                    session.execute(stmt)
                upsert_count += 1

        logger.info(
            "✅ [DB Warm-up] 완료 — 처리 %d봉, 지표 upsert %d건",
            len(rows), upsert_count
        )
        return upsert_count

    except Exception as exc:
        logger.error("❌ [DB Warm-up] 실패: %s", exc, exc_info=True)
        return 0


# ── 봇 최초 기동 시 DB Warm-up 자동 실행 ────────────────────────────────────
try:
    _warmup_symbol = "BTC/USDT"
    _warmed = _warmup_from_db(symbol=_warmup_symbol, lookback=500)
    logger.info(
        "🔄 [DB Warm-up] 시동 완료 — %d개 NULL 지표 레코드 복원", _warmed
    )
except Exception as _warmup_exc:
    logger.warning("⚠️ [DB Warm-up] 시동 중 Warm-up 실패 (봇 계속 가동): %s", _warmup_exc)


# ─────────────────────────────────────────────
# 📡 시세 데이터 수집 & DB 저장
# ─────────────────────────────────────────────
@celery_app.task(
    bind=True,
    name="worker.tasks.fetch_market_data_task",
    queue="market_data",
    max_retries=3,
    default_retry_delay=10,
)
def fetch_market_data_task(self, symbol: str = "BTC/USDT"):
    logger.info(f"📡 OHLCV 수집 및 피처 생성 시작: {symbol}")
    try:
        exchange = get_exchange()
        # ── [Timezone Fix] limit 500봉: EMA50(~50봉) + SMA20 warm-up 완전 보장 ──
        # 이전 limit=100은 봇 기동 직후 충분한 warm-up을 보장하지 못했음.
        # 500봉은 모든 지표(EMA50, Stochastic 등)의 min_periods를 여유롭게 충족.
        ohlcv_list = exchange.fetch_ohlcv(symbol=symbol, timeframe="1m", limit=500)

        if not ohlcv_list:
            logger.warning(f"⚠️ 빈 OHLCV 응답: {symbol}")
            return {"status": "empty", "symbol": symbol}

        df = pd.DataFrame(
            ohlcv_list,
            columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
        ).astype({"open": float, "high": float, "low": float, "close": float, "volume": float})

        # ── [Timezone Fix] timestamp_ms 기준 오름차순 정렬 보장 ──────────────
        # Binance OHLCV timestamp는 UTC milliseconds.
        # 정렬을 명시하여 타임존 혼재로 인한 순서 역전을 방지.
        df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

        # 피처 엔지니어링 파이프라인
        from worker.indicators import compute_all_features
        df = compute_all_features(df)

        import ta
        if len(df) < 26:
            logger.warning(f"⚠️ [실시간 수집] 데이터 캔들 수가 부족하여 지표 계산을 스킵합니다. (현재: {len(df)}봉)")
        else:
            try:
                df['atr_14'] = ta.volatility.average_true_range(high=df['high'], low=df['low'], close=df['close'], window=14)
                df['macd_line'] = ta.trend.macd(close=df['close'], window_fast=12, window_slow=26)
                df['macd_signal'] = ta.trend.macd_signal(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
                df['macd_hist'] = ta.trend.macd_diff(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
            except IndexError as e:
                logger.warning(f"⚠️ [실시간 수집] 지표 계산 중 IndexError 발생: {e}")
            except Exception as e:
                logger.warning(f"⚠️ [실시간 수집] 지표 계산 중 예외 발생: {e}")

        last = df.iloc[-1]
        # ── [Timezone Fix] UTC timezone-aware datetime 강제 보장 ─────────────
        candle_dt = _ensure_utc(int(last["timestamp_ms"]))

        # ── [NaN 가드] pd.isna() 기준으로 유효 숫자인지 사전 검증 ─────────────
        # _to_dec()만으로는 numpy NaN이 통과될 수 있으므로, 변환 전 명시적 체크.
        _raw_sma    = last.get("sma_20")
        _raw_rsi    = last.get("rsi_14")
        _raw_upper  = last.get("bb_upper")
        _raw_lower  = last.get("bb_lower")
        _raw_atr    = last.get("atr_14")
        _raw_macd_l = last.get("macd_line")
        _raw_macd_s = last.get("macd_signal")
        _raw_macd_h = last.get("macd_hist")

        sma_20   = _to_dec(_raw_sma)   if not pd.isna(_raw_sma)   else None
        rsi_14   = _to_dec(_raw_rsi)   if not pd.isna(_raw_rsi)   else None
        bb_upper = _to_dec(_raw_upper) if not pd.isna(_raw_upper) else None
        bb_lower = _to_dec(_raw_lower) if not pd.isna(_raw_lower) else None
        atr_14      = _to_dec(_raw_atr)    if not pd.isna(_raw_atr)    else None
        macd_line   = _to_dec(_raw_macd_l) if not pd.isna(_raw_macd_l) else None
        macd_signal = _to_dec(_raw_macd_s) if not pd.isna(_raw_macd_s) else None
        macd_hist   = _to_dec(_raw_macd_h) if not pd.isna(_raw_macd_h) else None

        insert_stmt = pg_insert(MarketData).values(
            timestamp=candle_dt,
            symbol=symbol,
            open=Decimal(str(last["open"])),
            high=Decimal(str(last["high"])),
            low=Decimal(str(last["low"])),
            close=Decimal(str(last["close"])),
            volume=Decimal(str(last["volume"])),
            sma_20=sma_20,
            rsi_14=rsi_14,
            bb_upper=bb_upper,
            bb_lower=bb_lower,
            atr_14=atr_14,
            macd_line=macd_line,
            macd_signal=macd_signal,
            macd_hist=macd_hist,
        )
        stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_market_data_ts_symbol",
            set_={
                "sma_20":   insert_stmt.excluded.sma_20,
                "rsi_14":   insert_stmt.excluded.rsi_14,
                "bb_upper": insert_stmt.excluded.bb_upper,
                "bb_lower": insert_stmt.excluded.bb_lower,
                "atr_14":   insert_stmt.excluded.atr_14,
                "macd_line": insert_stmt.excluded.macd_line,
                "macd_signal": insert_stmt.excluded.macd_signal,
                "macd_hist": insert_stmt.excluded.macd_hist,
            },
        )

        for session in _get_sync_session():
            session.execute(stmt)
        return {"status": "ok", "symbol": symbol, "timestamp": candle_dt.isoformat()}

    except Exception as exc:
        logger.error(f"❌ OHLCV 수집 실패: {exc}", exc_info=True)
        raise self.retry(exc=exc)


# ─────────────────────────────────────────────
# 🧠 하이브리드 청산 & 스나이퍼 매매 집행 (알림 연동)
# ─────────────────────────────────────────────
@celery_app.task(
    bind=True,
    name="worker.tasks.analyze_and_trade",
    queue="trading",
    max_retries=2,
    default_retry_delay=10,
)
def analyze_and_trade(self, symbol: str = "BTC/USDT"):
    logger.info(f"🧠 QuantFlow 하이브리드 의사결정 엔진 가동: {symbol}")

    try:
        now_utc = datetime.now(timezone.utc)
        exchange = get_exchange()
        
        # 1. 실시간 자산 잔고 트래킹 — 3회 자동 재시도 보장 (8차 확장)
        try:
            balance_info = _fetch_balance_with_retry(exchange)
            usdt_balance = Decimal(str(balance_info["total"].get("USDT", 0.0)))
            usdt_free    = Decimal(str(balance_info["free"].get("USDT", 0.0)))
            btc_free     = Decimal(str(balance_info["free"].get("BTC", 0.0)))
        except Exception as e:
            logger.error("❌ 실시간 지갑 잔고 조회 최종 실패 (%d회 재시도 소진): %s", _BALANCE_RETRY_MAX, e)
            return {"status": "balance_fetch_failed"}

        for session in _get_sync_session():
            # 2. 최신 시세 데이터 스캔
            latest = session.query(MarketData).filter(MarketData.symbol == symbol).order_by(desc(MarketData.timestamp)).first()
            if latest is None:
                logger.warning(f"⚠️ 분석 스킵 (MarketData 없음)")
                return {"status": "no_data"}

            # ── [Timezone Fix] latest 레코드의 지표 유효성 사전 검증 ────────────
            # rsi_14, bb_upper, bb_lower 중 하나라도 NULL이면 RuleBasedPredictor가
            # 즉시 HOLD를 반환하는 NaN 루프가 발생.
            # → 이 경우 DB의 최근 500봉을 즉시 재조회하여 지표를 실시간으로 채워줌.
            _indicators_valid = (
                latest.rsi_14 is not None
                and latest.bb_upper is not None
                and latest.bb_lower is not None
            )
            if not _indicators_valid:
                # ── 매 분 신규 캔들이 NULL 지표로 INSERT되는 것은 정상 파이프라인 흐름.
                # → DB 최근 500봉으로 지표를 재계산한 뒤, 유효한 숫자만 UPDATE 영구 저장.
                _hist_rows = (
                    session.query(MarketData)
                    .filter(MarketData.symbol == symbol)
                    .order_by(desc(MarketData.timestamp))
                    .limit(500)
                    .all()
                )
                if len(_hist_rows) < 20:
                    logger.warning(
                        "⚠️ [지표 패치] DB 데이터 부족(%d봉 < 20봉) — 분석 스킵",
                        len(_hist_rows),
                    )
                    return {"status": "insufficient_data"}

                _hist_rows.reverse()  # oldest → newest
                _df_hist = pd.DataFrame([
                    {
                        "timestamp_ms": int(
                            (_r.timestamp.astimezone(timezone.utc)
                             if _r.timestamp.tzinfo else
                             _r.timestamp.replace(tzinfo=timezone.utc)).timestamp() * 1000
                        ),
                        "open":   float(_r.open),
                        "high":   float(_r.high),
                        "low":    float(_r.low),
                        "close":  float(_r.close),
                        "volume": float(_r.volume),
                    }
                    for _r in _hist_rows
                ])
                _df_hist = _df_hist.sort_values(
                    "timestamp_ms", ascending=True
                ).reset_index(drop=True)
                from worker.indicators import compute_all_features as _caf
                _df_hist = _caf(_df_hist)

                # ── [Step 1] raw float 추출 ──────────────────────────────────
                _last_feat  = _df_hist.iloc[-1]
                _raw_rsi    = _last_feat.get("rsi_14")
                _raw_upper  = _last_feat.get("bb_upper")
                _raw_lower  = _last_feat.get("bb_lower")
                _raw_sma    = _last_feat.get("sma_20")

                # ── [Step 2] NaN 명시적 검증 — pd.isna() 기준 ───────────────
                # _to_dec()에 넘기기 전에 반드시 유효성을 확인해야 함.
                # pd.isna()는 float NaN / numpy NaN / None / pd.NA 모두 커버.
                # 핵심 3개 지표 중 하나라도 NaN이면 DB UPDATE를 절대 실행하지 않음.
                if pd.isna(_raw_rsi) or pd.isna(_raw_upper) or pd.isna(_raw_lower):
                    logger.warning(
                        "⚠️ [지표 패치] 재계산 후에도 NaN — 데이터 %d봉 부족, 분석 스킵",
                        len(_hist_rows),
                    )
                    return {"status": "insufficient_indicator_data"}

                # ── [Step 3] 유효한 숫자임이 확인된 값만 Decimal 변환 ─────────
                _patched_rsi   = _to_dec(_raw_rsi)
                _patched_upper = _to_dec(_raw_upper)
                _patched_lower = _to_dec(_raw_lower)
                _patched_sma   = _to_dec(_raw_sma)

                # _to_dec 이중 안전망: 변환 후에도 None이면 UPDATE 금지
                if _patched_rsi is None or _patched_upper is None or _patched_lower is None:
                    logger.warning(
                        "⚠️ [지표 패치] Decimal 변환 실패 — DB UPDATE 스킵 (id=%s)",
                        latest.id,
                    )
                    return {"status": "insufficient_indicator_data"}

                # ── [Step 4] 진짜 유효한 숫자만 독립 세션으로 DB UPDATE ────────
                # 동시성 격리: 공유 세션(session)을 건드리지 않고
                # 패치 전용 세션을 별도 생성하여 commit/rollback을 완전 격리.
                #
                # [커넥션 풀 누수 방어]
                # _patch_session = None 으로 선제 초기화 후 try 내부에서 생성.
                # → _SyncSession() 자체가 예외를 던지더라도 finally가 안전하게
                #   실행되어 is not None 체크 후 close() 를 보장.
                _patch_session = None
                try:
                    _patch_session = _SyncSession()
                    from sqlalchemy import update as sa_update
                    _patch_stmt = (
                        sa_update(MarketData)
                        .where(MarketData.id == latest.id)
                        .values(
                            rsi_14=_patched_rsi,
                            bb_upper=_patched_upper,
                            bb_lower=_patched_lower,
                            sma_20=_patched_sma,
                        )
                    )
                    _patch_session.execute(_patch_stmt)
                    _patch_session.commit()
                except Exception as _patch_db_exc:
                    if _patch_session is not None:
                        _patch_session.rollback()
                    logger.warning(
                        "⚠️ [지표 패치] DB UPDATE 실패 (id=%s): %s",
                        latest.id, _patch_db_exc,
                    )
                finally:
                    if _patch_session is not None:
                        _patch_session.close()

                # 메모리상 객체도 동기화 (현재 턴 분석 정상 진행)
                latest.rsi_14   = _patched_rsi
                latest.bb_upper = _patched_upper
                latest.bb_lower = _patched_lower
                latest.sma_20   = _patched_sma

            current_close = Decimal(str(latest.close))

            # 3. 가장 최근 체결 주문 기반 실시간 숏 포지션(SHORT/FLAT) 상태 분석
            last_trade = session.query(TradeHistory).filter(TradeHistory.symbol == symbol, TradeHistory.status == "FILLED").order_by(desc(TradeHistory.timestamp)).first()
            
            current_position = "FLAT"
            entry_price = None
            if last_trade and last_trade.side == "SELL":
                current_position = "SHORT"
                entry_price = Decimal(str(last_trade.price))

            # 4. 🛡️ [손절 방패 (우선순위 1위)] 리스크 관리 작동 검사
            if current_position == "SHORT" and entry_price:
                # 숏 포지션 수익률: 가격이 하락해야 수익 (+)
                price_return = (entry_price - current_close) / entry_price
                if price_return <= STOP_LOSS_THRESHOLD:
                    logger.warning(f"🚨 [손절 방패 가동] 평단가: {entry_price} -> 현재가: {current_close} ({price_return*100:.2f}%)")
                    
                    # [동적 수량 계산 - 손절 청산]: 숏 진입 시 수량만큼 환매수(BUY)
                    calculated_amount = Decimal(str(last_trade.amount))
                    
                    # 방어적 검증 (Short-circuit): 최소 주문 수량 미만 검사
                    if calculated_amount <= Decimal("0") or calculated_amount < MIN_ORDER_BTC:
                        logger.warning(
                            f"⏸️  [손절 방패] 계산된 청산 수량이 부족하여 주문 생략: "
                            f"계산된 수량={calculated_amount}, 최소 필요={MIN_ORDER_BTC}"
                        )
                        return {"status": "insufficient_calculated_amount"}

                    # 🚀 [Phase 3] 프로덕션 등급 주문 집행 파이프라인 호출 (손절 방패)
                    sl_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="BUY",  # 숏 포지션 청산은 BUY
                        amount=calculated_amount,
                        trigger_type="STOP_LOSS_SHIELD",
                        fallback_price=current_close,
                        confidence=1.0,
                        usdt_balance=usdt_balance,
                    )

                    # 이력 저장 — 실제 체결 수량/단가 기준으로 칼정산
                    sl_record_amount = sl_result["filled_amount"] if sl_result["filled_amount"] > Decimal("0") else calculated_amount
                    trade_record = TradeHistory(
                        timestamp=datetime.now(timezone.utc), symbol=symbol, side="SELL",
                        price=sl_result["filled_price"], amount=sl_record_amount,
                        status=sl_result["status"],
                    )
                    session.add(trade_record)
                    logger.info(
                        "🗄️ [STOP_LOSS_SHIELD] DB 이력 저장: status=%s, order_id=%s",
                        sl_result["status"], sl_result["order_id"]
                    )

                    return {"status": f"stop_loss_{sl_result['status'].lower()}", "order_id": sl_result["order_id"]}

            # 4-1. ⏱️ [8차/9.1.0 확장] 하드 TP / 하드 SL / 타임아웃 안전장치 (우선순위 2위)
            # ─────────────────────────────────────────────────────────────────────
            # 지표 신호와 무관하게, 포지션이 극단적 수익/손실에 도달하거나
            # 최대 보유 시간을 초과하면 무조건 강제 청산합니다.
            if current_position == "SHORT" and entry_price and last_trade:
                # 숏 포지션 수익률 (진입가 - 현재가) / 진입가
                price_return = (entry_price - current_close) / entry_price
                now_utc_check = datetime.now(timezone.utc)

                # 진입 시각 타임존 정규화
                entry_ts = last_trade.timestamp
                if entry_ts.tzinfo is None:
                    entry_ts = entry_ts.replace(tzinfo=timezone.utc)
                else:
                    entry_ts = entry_ts.astimezone(timezone.utc)
                minutes_held = (now_utc_check - entry_ts).total_seconds() / 60.0

                _hard_trigger: str | None = None
                _hard_reason: str = ""

                if minutes_held >= MAX_POSITION_MINUTES:
                    _hard_trigger = "TIMEOUT_EXIT"
                    _hard_reason  = f"보유 {minutes_held:.0f}분 → 최대 {MAX_POSITION_MINUTES}분 초과"
                elif price_return >= HARD_TP_THRESHOLD:
                    _hard_trigger = "HARD_TP_EXIT"
                    _hard_reason  = f"숏 수익률 {price_return*100:+.2f}% ≥ +{float(HARD_TP_THRESHOLD)*100:.1f}% 하드 익절"
                elif price_return <= HARD_SL_THRESHOLD:
                    _hard_trigger = "HARD_SL_EXIT"
                    _hard_reason  = f"숏 수익률 {price_return*100:+.2f}% ≤ {float(HARD_SL_THRESHOLD)*100:.1f}% 하드 손절"

                if _hard_trigger:
                    logger.warning(
                        "🔔 [%s] 숏 강제 청산 발동: %s (평단=$%s, 현재=$%s)",
                        _hard_trigger, _hard_reason, entry_price, current_close,
                    )
                    _hard_amount = Decimal(str(last_trade.amount))
                    if _hard_amount <= Decimal("0") or _hard_amount < MIN_ORDER_BTC:
                        logger.warning("[%s] 숏 청산 수량 부족 → 청산 스킵: %s", _hard_trigger, _hard_amount)
                        return {"status": "insufficient_amount_for_hard_exit"}

                    _hard_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="BUY",  # 숏 청산
                        amount=_hard_amount,
                        trigger_type=_hard_trigger,
                        fallback_price=current_close,
                        confidence=1.0,
                        usdt_balance=usdt_balance,
                    )
                    _hard_rec_amount = (
                        _hard_result["filled_amount"]
                        if _hard_result["filled_amount"] > Decimal("0")
                        else _hard_amount
                    )
                    session.add(TradeHistory(
                        timestamp=datetime.now(timezone.utc), symbol=symbol, side="BUY",
                        price=_hard_result["filled_price"], amount=_hard_rec_amount,
                        status=_hard_result["status"],
                    ))
                    # 텔레그램 강제 청산 알림
                    notifier.send_message(
                        f"🔔 <b>[QuantFlow] {_hard_trigger} (Short Cover)</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                        f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                        f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                        f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                        f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━"
                    )
                    return {"status": f"{_hard_trigger.lower()}_{_hard_result['status'].lower()}", "order_id": _hard_result["order_id"]}

            # 5. 🎯 [v9.1.0] 하드코딩된 숏(Short) 매매 판단 (Predictor 오버라이드)
            current_rsi = Decimal(str(latest.rsi_14)) if latest.rsi_14 is not None else Decimal("50")
            current_bb_upper = Decimal(str(latest.bb_upper)) if latest.bb_upper is not None else Decimal("0")
            current_bb_lower = Decimal(str(latest.bb_lower)) if latest.bb_lower is not None else Decimal("0")
            
            action = "HOLD"
            confidence = 1.0
            
            if current_position == "SHORT":
                # 청산 버퍼 확장: bb_lower 터치 혹은 rsi <= 35
                if current_close <= current_bb_lower or current_rsi <= Decimal("35"):
                    logger.info(f"🎯 [v9.1.0] 숏 청산 시그널 발생! Close({current_close}) <= BB_Lower({current_bb_lower}) OR RSI({current_rsi}) <= 35")
                    action = "BUY"  # 숏 청산(환매수)
            else:
                # 진입 타점 강화: bb_upper 돌파 + rsi >= 73
                if current_close > current_bb_upper and current_rsi >= Decimal("73"):
                    logger.info(f"🎯 [v9.1.0] 숏 진입 시그널 발생! Close({current_close}) > BB_Upper({current_bb_upper}) AND RSI({current_rsi}) >= 73")
                    action = "SELL" # 숏 진입

            if action == "HOLD":
                return {"status": "hold_or_low_confidence", "confidence": confidence}

            # 6. 📈 피라미딩(Pyramiding) 매매 로직 (수학적 평단가 방어)
            PRICE_BUFFER_PCT = Decimal("0.005")  # 0.5%
            
            # 최근 5분 이내 동일 방향 거래 이력 조회
            cooldown_cutoff = now_utc - timedelta(minutes=COOLDOWN_MINUTES)
            recent_same_side = session.query(TradeHistory).filter(
                TradeHistory.symbol == symbol, TradeHistory.side == action, TradeHistory.timestamp >= cooldown_cutoff
            ).order_by(desc(TradeHistory.timestamp)).first()

            if recent_same_side is not None:
                last_price = Decimal(str(recent_same_side.price))
                
                if action == "SELL":
                    # 숏 추가 진입: 현재가가 직전 체결가보다 최소 0.5% 이상 높아야 유리한 평단가 방어 가능
                    required_price = last_price * (Decimal("1") + PRICE_BUFFER_PCT)
                    if current_close < required_price:
                        logger.warning(f"⏳ [피라미딩 가드] {action} 스킵: 현재가({current_close:.2f})가 조건({required_price:.2f})에 미달하여 유리한 단가가 아님.")
                        return {"status": "pyramiding_skipped"}
                elif action == "BUY":
                    # 숏 분할 청산: 현재가가 직전 체결가보다 최소 0.5% 이상 낮아야 유리함
                    required_price = last_price * (Decimal("1") - PRICE_BUFFER_PCT)
                    if current_close > required_price:
                        logger.warning(f"⏳ [피라미딩 가드] {action} 스킵: 현재가({current_close:.2f})가 조건({required_price:.2f})에 미달하여 유리한 단가가 아님.")
                        return {"status": "pyramiding_skipped"}
                        
                logger.info(f"📈 [피라미딩 통과] 단가 방어 완료! {action} 연속 진입 승인 (현재가: {current_close:.2f} / 직전가: {last_price:.2f})")

            # 7. ⚡ 주문 분기점 정의 (신규 숏 진입 vs 숏 청산) 및 동적 주문 수량 계산
            trigger_type = "SNIPER_SHORT_ENTRY"
            if current_position == "SHORT" and action == "BUY":
                trigger_type = "REVERSE_SWITCH_EXIT_SHORT"

            # 동적 주문 수량 연산 (USDT 10% 숏 진입 vs 진입수량 환매수 청산)
            if action == "SELL":
                # 가용 USDT의 10%만큼 숏 진입 수량 계산
                target_usdt = usdt_free * Decimal("0.1")
                calculated_amount = target_usdt / current_close
                logger.info(f"💰 [자산 배분 - 숏 진입] 가용 USDT: {usdt_free} -> 진입 목표: {target_usdt} USDT -> 계산 수량: {calculated_amount} BTC")
            elif action == "BUY":
                # 숏 청산 시 진입했던 수량만큼 환매수
                calculated_amount = Decimal(str(last_trade.amount)) if last_trade else Decimal("0")
                logger.info(f"💰 [자산 배분 - 숏 청산] 보유 숏 전량 청산: {calculated_amount} BTC")
            else:
                calculated_amount = Decimal("0")

            # 방어적 검증 (Short-circuit): 수량 부족 시 주문 취소 및 조기 리턴
            if calculated_amount <= Decimal("0") or calculated_amount < MIN_ORDER_BTC:
                logger.warning(
                    f"⏸️  [{trigger_type}] 계산된 주문 수량이 부족하여 주문 생략: "
                    f"수량={calculated_amount}, 최소 필요={MIN_ORDER_BTC}"
                )
                return {"status": "insufficient_calculated_amount"}

            # 8. 🚀 [Phase 3] 프로덕션 등급 주문 집행 파이프라인 호출 (스나이퍼/역시그널)
            logger.info(f"🚀 [{trigger_type}] 주문 집행 파이프라인 진입: {action} {calculated_amount} BTC")

            exec_result: OrderResult = _execute_order_pipeline(
                exchange=exchange,
                symbol=symbol,
                side=action,
                amount=calculated_amount,
                trigger_type=trigger_type,
                fallback_price=current_close,
                confidence=confidence,
                usdt_balance=usdt_balance,
            )

            # 이력 저장 — 실제 체결 수량/단가 기준으로 칼정산
            # PARTIALLY_FILLED 시 실 체결 수량, 그 외 미체결(CANCELLED/FAILED) 시 요청 수량 기준 기록
            record_amount = (
                exec_result["filled_amount"]
                if exec_result["filled_amount"] > Decimal("0")
                else calculated_amount
            )
            trade_record = TradeHistory(
                timestamp=now_utc, symbol=symbol, side=action,
                price=exec_result["filled_price"], amount=record_amount,
                status=exec_result["status"],
            )
            session.add(trade_record)
            logger.info(
                "🗄️ [%s] DB 이력 저장: status=%s, filled_amount=%s, order_id=%s",
                trigger_type, exec_result["status"], exec_result["filled_amount"], exec_result["order_id"]
            )

            return {
                "status": exec_result["status"].lower(),
                "order_id": exec_result["order_id"],
                "trigger_type": trigger_type,
                "filled_amount": str(exec_result["filled_amount"]),
                "filled_price": str(exec_result["filled_price"]),
            }

    except Exception as exc:
        logger.error(f"❌ 매매 파이프라인 엔진 내 런타임 예외 발생: {exc}", exc_info=True)
        raise self.retry(exc=exc)


# ⏱️ NTP 시간 동기화 및 스켈레톤 함수 유지
@celery_app.task(name="worker.tasks.check_time_sync_task", queue="default")
def check_time_sync_task():
    return {"ntp_drift_ms": round(check_ntp_drift(), 1)}

@celery_app.task(name="worker.tasks.daily_report_task", queue="default")
def daily_report_task():
    """
    📊 QuantFlow 일간 결산 리포트 — Celery Beat에 의해 매일 자정(UTC) 호출.
    지난 24시간 매매 성과를 정밀 집계하여 텔레그램 HTML 리포트로 발송합니다.
    """
    logger.info("📊 [일간 결산] 리포트 생성 파이프라인 가동")

    try:
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=1)

        for session in _get_sync_session():
            # ── 1. 지난 24시간 전체 매매 내역 스캔 ──────────────────────────
            trades = (
                session.query(TradeHistory)
                .filter(TradeHistory.timestamp >= cutoff)
                .order_by(TradeHistory.timestamp)
                .all()
            )

            # [Short-circuit] 매매 이력 없으면 스킵 메시지 전송 후 조기 리턴
            if not trades:
                logger.info("📊 지난 24시간 매매 이력 없음 — 리포트 스킵")
                notifier.send_message(
                    "📊 <b>[QuantFlow 일간 결산]</b>\n"
                    "지난 24시간 동안 발생한 매매 이력이 없어 리포트 생성을 스킵합니다."
                )
                return {"status": "skipped_no_trades"}

            # ── 2. 기본 집계 지표 산출 ─────────────────────────────────────
            total_count = len(trades)
            filled_trades = [t for t in trades if t.status == "FILLED"]
            filled_count = len(filled_trades)
            rejected_count = sum(1 for t in trades if t.status == "REJECTED")
            failed_count = sum(1 for t in trades if t.status == "FAILED")

            # ── 3. 간이 PnL 모델 (Σ SELL 체결총액 − Σ BUY 체결총액) ───────
            total_buy_value = Decimal("0")
            total_sell_value = Decimal("0")
            total_buy_amount = Decimal("0")
            total_sell_amount = Decimal("0")

            for t in filled_trades:
                # 가격 또는 수량이 None인 주문은 안전하게 제외
                if t.price is None or t.amount is None:
                    continue
                trade_value = Decimal(str(t.price)) * Decimal(str(t.amount))
                if t.side == "BUY":
                    total_buy_value += trade_value
                    total_buy_amount += Decimal(str(t.amount))
                elif t.side == "SELL":
                    total_sell_value += trade_value
                    total_sell_amount += Decimal(str(t.amount))

            realized_pnl = total_sell_value - total_buy_value

            # ── 4. 승률(Win Rate) 산출 ────────────────────────────────────
            #   개별 SELL 건별로 "직전 BUY 체결 평단가" 대비 수익 여부 판정
            #   직전 BUY가 없는 SELL은 판정 불가로 제외
            filled_buys = [t for t in filled_trades if t.side == "BUY" and t.price is not None]
            filled_sells = [t for t in filled_trades if t.side == "SELL" and t.price is not None]

            win_count = 0
            loss_count = 0

            for sell_trade in filled_sells:
                # 해당 SELL 시점 이전의 모든 BUY 체결가에서 가중 평단가 산출
                prior_buys = [
                    b for b in filled_buys
                    if b.timestamp <= sell_trade.timestamp and b.symbol == sell_trade.symbol
                ]
                if not prior_buys:
                    continue  # 직전 매수 이력 없으면 승패 판정 불가 → 스킵

                # 가중 평균 매수 단가 = Σ(price × amount) / Σ(amount)
                sum_cost = Decimal("0")
                sum_qty = Decimal("0")
                for b in prior_buys:
                    sum_cost += Decimal(str(b.price)) * Decimal(str(b.amount))
                    sum_qty += Decimal(str(b.amount))

                if sum_qty == Decimal("0"):
                    continue

                avg_buy_price = sum_cost / sum_qty
                sell_price = Decimal(str(sell_trade.price))

                if sell_price > avg_buy_price:
                    win_count += 1
                else:
                    loss_count += 1

            evaluated_total = win_count + loss_count
            win_rate_str = f"{(win_count / evaluated_total * 100):.1f}%" if evaluated_total > 0 else "N/A (판정 대상 없음)"

            # ── 5. 체결 성공률 ─────────────────────────────────────────────
            fill_rate_str = f"{(filled_count / total_count * 100):.1f}%" if total_count > 0 else "N/A"

            # ── 6. PnL 이모지 및 부호 표기 ─────────────────────────────────
            if realized_pnl > Decimal("0"):
                pnl_emoji = "📈"
                pnl_sign = "+"
            elif realized_pnl < Decimal("0"):
                pnl_emoji = "📉"
                pnl_sign = ""  # Decimal 음수는 자동으로 '-' 포함
            else:
                pnl_emoji = "➖"
                pnl_sign = ""

            # ── 7. KST 시간대 표기 ────────────────────────────────────────
            kst_tz = timezone(timedelta(hours=9))
            report_time_kst = datetime.now(kst_tz).strftime("%Y-%m-%d %H:%M:%S KST")
            cutoff_kst = cutoff.astimezone(kst_tz).strftime("%Y-%m-%d %H:%M")
            now_kst = now_utc.astimezone(kst_tz).strftime("%Y-%m-%d %H:%M")

            # ── 8. 프리미엄 HTML 결산 리포트 조립 ─────────────────────────
            report_lines = [
                "📊 <b>[QuantFlow 일간 결산 리포트]</b>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>집계 구간:</b> <code>{cutoff_kst} ~ {now_kst}</code>",
                "",
                "📋 <b>매매 활동 요약</b>",
                "────────────────────",
                f"• <b>총 주문 시도:</b> <code>{total_count:,}건</code>",
                f"• <b>체결 완료(FILLED):</b> <code>{filled_count:,}건</code>",
                f"• <b>거부(REJECTED):</b> <code>{rejected_count:,}건</code>",
                f"• <b>실패(FAILED):</b> <code>{failed_count:,}건</code>",
                f"• <b>체결 성공률:</b> <code>{fill_rate_str}</code>",
                "",
                "💰 <b>매매 성과 지표</b>",
                "────────────────────",
                f"• <b>총 매수 체결액:</b> <code>${total_buy_value:,.2f} USDT</code>",
                f"• <b>총 매도 체결액:</b> <code>${total_sell_value:,.2f} USDT</code>",
                f"• {pnl_emoji} <b>실현 손익(PnL):</b> <code>{pnl_sign}{realized_pnl:,.2f} USDT</code>",
                "",
                "🎯 <b>승률 분석</b>",
                "────────────────────",
                f"• <b>익절(Win):</b> <code>{win_count:,}건</code>",
                f"• <b>손절(Loss):</b> <code>{loss_count:,}건</code>",
                f"• <b>승률(Win Rate):</b> <code>{win_rate_str}</code>",
                "",
                "📦 <b>수량 요약</b>",
                "────────────────────",
                f"• <b>총 매수 수량:</b> <code>{total_buy_amount:,.4f} BTC</code>",
                f"• <b>총 매도 수량:</b> <code>{total_sell_amount:,.4f} BTC</code>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>리포트 생성:</b> <code>{report_time_kst}</code>",
            ]
            report_message = "\n".join(report_lines)

            # ── 9. 텔레그램 전송 ──────────────────────────────────────────
            notifier.send_message(report_message)
            logger.info(f"📊 [일간 결산] 리포트 전송 완료 — 총 {total_count}건, PnL: {pnl_sign}{realized_pnl:,.2f} USDT")

            return {
                "status": "report_sent",
                "total_trades": total_count,
                "filled_trades": filled_count,
                "realized_pnl": str(realized_pnl),
                "win_rate": win_rate_str,
            }

    except Exception as exc:
        logger.error(f"❌ [일간 결산] 리포트 생성 중 예외 발생: {exc}", exc_info=True)
        # 리포트 실패가 전체 워커를 죽이지 않도록 안전하게 에러 상태만 반환
        return {"status": "report_failed", "error": str(exc)}


# ─────────────────────────────────────────────
# 📥 [8차 확장] 텔레그램 실시간 /status 명령어 및 일일 결산 스케줄러
# ─────────────────────────────────────────────

def _send_status_brief():
    """
    📊 실시간 시스템 관제 요약 브리핑을 조립하여 텔레그램으로 전송합니다.
    """
    try:
        exchange = get_exchange()
        # [모의투자 모드 동기화]
        if getattr(settings, "BINANCE_SANDBOX_MODE", False):
            exchange.set_sandbox_mode(True)
            
        balance_info = _fetch_balance_with_retry(exchange)
        usdt_free = Decimal(str(balance_info["free"].get("USDT", 0.0)))
        total_assets = Decimal(str(balance_info["total"].get("USDT", 0.0)))
        
        symbol = "BTC/USDT"
        
        for session in _get_sync_session():
            latest = session.query(MarketData).filter(
                MarketData.symbol == symbol
            ).order_by(desc(MarketData.timestamp)).first()
            
            if not latest:
                notifier.send_message("📊 <b>[QuantFlow 관제 브리핑]</b>\n시스템에 시장 데이터가 존재하지 않습니다.")
                return
            
            current_close = Decimal(str(latest.close))
            rsi_val = float(latest.rsi_14) if latest.rsi_14 is not None else 0.0
            bb_upper_val = float(latest.bb_upper) if latest.bb_upper is not None else float(current_close)
            bb_lower_val = float(latest.bb_lower) if latest.bb_lower is not None else float(current_close)
            sma_val = float(latest.sma_20) if latest.sma_20 is not None else float(current_close)

            logger.debug(
                "📊 [브리핑 디버그] close=%.2f | sma_20=%s | bb_upper=%s | bb_lower=%s | rsi=%.2f",
                float(current_close),
                latest.sma_20,
                latest.bb_upper,
                latest.bb_lower,
                rsi_val,
            )

            # 포지션 분석
            last_trade = session.query(TradeHistory).filter(
                TradeHistory.symbol == symbol, TradeHistory.status == "FILLED"
            ).order_by(desc(TradeHistory.timestamp)).first()
            
            current_position = "FLAT"
            entry_price = None
            pos_pnl_str = ""
            pos_duration_str = ""
            
            # CCXT 실시간 포지션 조회 시도 (선물 계좌의 실제 상태 우선)
            ccxt_pos = None
            try:
                if exchange.has.get('fetchPositions'):
                    positions = exchange.fetch_positions([symbol])
                    for p in positions:
                        if p.get('contracts') and float(p['contracts']) > 0:
                            ccxt_pos = p
                            break
            except Exception as e:
                logger.warning(f"⚠️ CCXT 포지션 조회 실패 (Fallback to DB): {e}")

            if ccxt_pos:
                # CCXT 기준 포지션 파싱
                side_str = ccxt_pos.get('side', '').lower()
                current_position = "SHORT" if side_str == 'short' else "LONG"
                entry_price = Decimal(str(ccxt_pos.get('entryPrice', 0)))
                pos_amount = Decimal(str(ccxt_pos.get('contracts', 0)))
                
                # 바이낸스 선물(USDM-Futures) 실제 언리얼라이즈드 PNL 및 ROI 기반 수익률 추출
                if ccxt_pos.get('percentage') is not None:
                    pos_return = float(ccxt_pos['percentage'])
                else:
                    unrealized = float(ccxt_pos.get('unrealizedPnl', 0.0))
                    pos_value = float(entry_price * pos_amount)
                    if pos_value > 0:
                        pos_return = (unrealized / pos_value) * 100
                    else:
                        if current_position == "SHORT":
                            pos_return = float((entry_price - current_close) / entry_price * 100)
                        else:
                            pos_return = float((current_close - entry_price) / entry_price * 100)
                            
                pos_pnl_str = f" ({pos_return:+.2f}%)"
                
                # 진입 시간 경과 (DB의 마지막 거래 이력 참조)
                pos_duration_str = ""
                if last_trade:
                    entry_ts = last_trade.timestamp
                    if entry_ts.tzinfo is None:
                        entry_ts = entry_ts.replace(tzinfo=timezone.utc)
                    else:
                        entry_ts = entry_ts.astimezone(timezone.utc)
                    minutes_held = (datetime.now(timezone.utc) - entry_ts).total_seconds() / 60.0
                    pos_duration_str = f" ({minutes_held:.0f}분 보유)"
            else:
                # Fallback: DB 기준 로직 (v9.1.0 숏 진입 기준)
                if last_trade and last_trade.side == "SELL":
                    current_position = "SHORT"
                    entry_price = Decimal(str(last_trade.price))
                    pos_return = (entry_price - current_close) / entry_price * 100
                    pos_pnl_str = f" ({pos_return:+.2f}%)"
                    
                    entry_ts = last_trade.timestamp
                    if entry_ts.tzinfo is None:
                        entry_ts = entry_ts.replace(tzinfo=timezone.utc)
                    else:
                        entry_ts = entry_ts.astimezone(timezone.utc)
                    minutes_held = (datetime.now(timezone.utc) - entry_ts).total_seconds() / 60.0
                    pos_duration_str = f" ({minutes_held:.0f}분 보유)"

            # 최근 24시간 실현 손익 집계
            cutoff = datetime.now(timezone.utc) - timedelta(days=1)
            trades_24h = (
                session.query(TradeHistory)
                .filter(TradeHistory.timestamp >= cutoff, TradeHistory.status == "FILLED")
                .all()
            )
            
            total_buy_val = Decimal("0")
            total_sell_val = Decimal("0")
            for t in trades_24h:
                if t.price is None or t.amount is None:
                    continue
                val = Decimal(str(t.price)) * Decimal(str(t.amount))
                if t.side == "BUY":
                    total_buy_val += val
                elif t.side == "SELL":
                    total_sell_val += val
            realized_pnl_24h = total_sell_val - total_buy_val
            pnl_24h_str = f"{realized_pnl_24h:+,.2f} USDT"
            
            # 포지션 이모지
            if current_position == "SHORT":
                pos_emoji = "🔴 SHORT"
            elif current_position == "LONG":
                pos_emoji = "🟢 LONG"
            else:
                pos_emoji = "⚪ FLAT"
            
            # KST 시간 표기
            kst_tz = timezone(timedelta(hours=9))
            now_kst = datetime.now(kst_tz).strftime("%Y-%m-%d %H:%M:%S KST")
            
            brief_lines = [
                "📊 <b>[QuantFlow 실시간 관제 브리핑]</b>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• 🤖 <b>엔진 상태:</b> <code>RUNNING</code>",
                f"• ⏰ <b>관제 시각:</b> <code>{now_kst}</code>",
                "",
                "📦 <b>현재 유지 포지션</b>",
                "────────────────────",
                f"• <b>상태:</b> <b>{pos_emoji}</b>{pos_pnl_str}",
                f"• <b>평단가:</b> <code>${float(entry_price or 0):,.2f} USDT</code>" if entry_price else "• <b>평단가:</b> <code>-</code>",
                f"• <b>경과 시간:</b> <code>{pos_duration_str or '-'}</code>" if entry_price else "• <b>경과 시간:</b> <code>-</code>",
                "",
                "💰 <b>실시간 자산 잔고 (선물 지갑)</b>",
                "────────────────────",
                f"• <b>가용 잔고 (Free):</b> <code>${float(usdt_free):,.2f} USDT</code>",
                f"• <b>총 자산 (Total):</b> <code>${float(total_assets):,.2f} USDT</code>",
                f"• <b>24H 실현 손익:</b> <code>{pnl_24h_str}</code>",
                "",
                "📈 <b>실시간 시장 지표</b>",
                "────────────────────",
                f"• <b>현재가 (Close):</b> <code>${float(current_close):,.2f}</code>",
                f"• <b>RSI (14주기):</b> <code>{rsi_val:.2f}</code>",
                f"• <b>볼린저 밴드 상단:</b> <code>${bb_upper_val:,.2f}</code>",
                f"• <b>볼린저 밴드 하단:</b> <code>${bb_lower_val:,.2f}</code>",
                f"• <b>SMA (20주기):</b> <code>${sma_val:,.2f}</code>",
                "━━━━━━━━━━━━━━━━━━━━"
            ]
            
            notifier.send_message("\n".join(brief_lines))
            logger.info("✅ 텔레그램 '/status' 브리핑 발송 완료")
    except Exception as e:
        logger.error(f"❌ _send_status_brief() 중 에러 발생: {e}", exc_info=True)
        notifier.send_message(f"❌ <b>[QuantFlow 오류]</b>\n실시간 상태 브리핑 작성 중 실패했습니다: <code>{str(e)}</code>")

import unicodedata
import requests

def handle_telegram_command(command: str) -> str:
    """
    텔레그램 챗봇으로부터 수신된 명령어를 처리하고 Redis 상태를 제어하며,
    비동기 세션 데드락 방지를 위해 독립적인 HTTP 소켓(requests)으로 응답을 즉시 선제 발송합니다.
    """
    import redis
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    status_key = "quantflow:sys_status"
    
    # 2. 정돈 처리 강화: 전각 문자를 반각으로 변환(NFKC), 공백 제거, 소문자화하여 대소문자 혼용 및 특수문자 방어
    normalized_cmd = unicodedata.normalize('NFKC', command).strip().lower()
    
    token = getattr(settings, "telegram_bot_token", getattr(settings, "TELEGRAM_BOT_TOKEN", None))
    chat_id = getattr(settings, "telegram_chat_id", getattr(settings, "TELEGRAM_CHAT_ID", None))
    
    def _send_reply(text: str):
        if token and chat_id:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                payload = {"chat_id": chat_id, "text": text}
                response = requests.post(url, json=payload, timeout=5)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"❌ 텔레그램 응답 독립 소켓 통신 중 치명적 예외 발생 (시스템 다운 방어): {e}")

    # 1. 조건문 유연화: 'in' 연산자로 변경하여 이모지 섞임이나 포맷 변형 방어
    if "/pause" in normalized_cmd:
        r.set(status_key, "PAUSED")
        logger.warning("🛑 [원격 제어] 시스템이 사용자에 의해 일시 정지(PAUSED) 상태로 전환되었습니다.")
        _send_reply("⏸️ [QuantFlow] 시스템이 일시 정지되었습니다. 모든 자동 매매 분석 및 진입이 동결됩니다.")
        return "PAUSED"

    elif "/resume" in normalized_cmd:
        r.set(status_key, "RUNNING")
        logger.info("▶️ [원격 제어] 시스템이 사용자에 의해 재개(RUNNING) 상태로 전환되었습니다.")
        _send_reply("▶️ [QuantFlow] 시스템이 재개되었습니다. AI 하이브리드 사냥을 다시 집행합니다.")
        return "RUNNING"

    return "UNKNOWN_COMMAND"


@celery_app.task(name="worker.tasks.telegram_command_listener_task", queue="default")
def telegram_command_listener_task():
    """
    📥 텔레그램 명령어 수신기 — 30초마다 Celery Beat에 의해 가동.
    Telegram getUpdates API를 폴링하여 '/status' 명령어를 감지하고 실시간 상태를 브로드캐스팅합니다.
    """
    if not notifier.is_enabled:
        logger.debug("Telegram notifier is disabled. Skipping command listener.")
        return {"status": "notifier_disabled"}

    import redis
    import httpx
    
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    
    last_update_id_key = "quantflow:telegram:last_update_id"
    last_update_id = r.get(last_update_id_key)
    
    url = f"https://api.telegram.org/bot{notifier.bot_token}/getUpdates"
    params = {}
    if last_update_id:
        params["offset"] = int(last_update_id) + 1
    params["timeout"] = 5  # short polling timeout in seconds
    
    try:
        with httpx.Client() as client:
            response = client.get(url, params=params, timeout=10.0)
            if response.status_code != 200:
                logger.warning(f"Telegram getUpdates failed: HTTP {response.status_code}")
                return {"status": f"failed_http_{response.status_code}"}
            
            data = response.json()
            updates = data.get("result", [])
            if not updates:
                return {"status": "no_new_updates"}
            
            max_update_id = last_update_id
            for update in updates:
                update_id = update.get("update_id")
                if last_update_id and update_id <= int(last_update_id):
                    continue
                try:
                    safe_max_id = int(max_update_id) if max_update_id is not None else 0
                    safe_update_id = int(update_id)
                    max_update_id = max(safe_max_id, safe_update_id)
                except (ValueError, TypeError) as e:
                    logger.warning(f"텔레그램 update_id 형변환 실패: max_update_id={max_update_id}, update_id={update_id}, error={e}")
                    # 파싱 실패시 현재 처리한 update_id라도 (문자열로) 캐싱하여 무한 루프를 방지합니다.
                    max_update_id = str(update_id) if update_id is not None else max_update_id
                
                message = update.get("message")
                if not message:
                    continue
                
                chat = message.get("chat", {})
                chat_id = str(chat.get("id"))
                
                # chat_id 보안 가드: 설정된 chat_id와 일치하는 경우에만 명령을 처리
                if chat_id != str(notifier.chat_id):
                    logger.warning(f"Unauthorized chat_id attempt: {chat_id}")
                    continue
                
                text = message.get("text", "").strip()
                if text == "/status":
                    logger.info("🎯 텔레그램 '/status' 원격 명령어 수신 성공! 실시간 브리핑 작성 및 송신")
                    _send_status_brief()
                else:
                    # 유연한 명령어 훅 (이모지 및 변형 텍스트 대응)
                    res = handle_telegram_command(text)
                    if res != "UNKNOWN_COMMAND":
                        logger.info(f"✅ 원격 제어 명령어 처리 완료: {res}")
            
            if max_update_id:
                r.set(last_update_id_key, max_update_id)
                
            return {"status": "processed", "processed_count": len(updates)}
    except Exception as exc:
        logger.error(f"❌ 텔레그램 getUpdates 조회 중 예외 발생: {exc}", exc_info=True)
        return {"status": "error", "message": str(exc)}


@celery_app.task(name="worker.tasks.generate_daily_report_task", queue="default")
def generate_daily_report_task():
    """
    📊 QuantFlow 프리미엄 일일결산 리포트 스케줄러.
    매일 23:59 KST에 1회 호출되어 오늘 하루의 실현 손익, 자산 총액, 승률 등을 금융기관 수준으로 리포팅합니다.
    """
    logger.info("📊 [일간 결산] generate_daily_report_task 가동")
    
    try:
        kst_tz = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst_tz)
        
        # 오늘 KST 00:00:00 계산 후 UTC로 변환하여 DB 조회
        start_of_day_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_day_utc = start_of_day_kst.astimezone(timezone.utc)
        
        # 실시간 자산 및 시세 조회
        exchange = get_exchange()
        balance_info = _fetch_balance_with_retry(exchange)
        usdt_balance = Decimal(str(balance_info["total"].get("USDT", 0.0)))
        btc_balance = Decimal(str(balance_info["total"].get("BTC", 0.0)))
        
        symbol = "BTC/USDT"
        
        for session in _get_sync_session():
            latest = session.query(MarketData).filter(
                MarketData.symbol == symbol
            ).order_by(desc(MarketData.timestamp)).first()
            
            current_close = Decimal(str(latest.close)) if latest else Decimal("0")
            total_assets = usdt_balance + (btc_balance * current_close)
            
            # 오늘 KST 00:00 이후 체결된 주문 스캔
            today_trades = (
                session.query(TradeHistory)
                .filter(TradeHistory.timestamp >= start_of_day_utc)
                .order_by(TradeHistory.timestamp)
                .all()
            )
            
            # 오늘 체결 완료된 SELL 주문
            today_filled_sells = [
                t for t in today_trades
                if t.side == "SELL" and t.status == "FILLED" and t.price is not None
            ]
            
            # 매수 평단가 산출을 위해 symbol의 전체 BUY 이력 로드
            all_filled_buys = (
                session.query(TradeHistory)
                .filter(TradeHistory.symbol == symbol, TradeHistory.side == "BUY", TradeHistory.status == "FILLED")
                .order_by(TradeHistory.timestamp)
                .all()
            )
            
            # PnL 및 승률 계산 (Round-trip 매수 평단가 기준)
            realized_pnl = Decimal("0")
            win_count = 0
            loss_count = 0
            
            for sell_trade in today_filled_sells:
                prior_buys = [b for b in all_filled_buys if b.timestamp <= sell_trade.timestamp]
                if not prior_buys:
                    continue
                
                sum_cost = Decimal("0")
                sum_qty = Decimal("0")
                for b in prior_buys:
                    sum_cost += Decimal(str(b.price)) * Decimal(str(b.amount))
                    sum_qty += Decimal(str(b.amount))
                
                if sum_qty == Decimal("0"):
                    continue
                
                avg_buy_price = sum_cost / sum_qty
                sell_price = Decimal(str(sell_trade.price))
                sell_qty = Decimal(str(sell_trade.amount))
                
                trade_pnl = (sell_price - avg_buy_price) * sell_qty
                realized_pnl += trade_pnl
                
                if sell_price > avg_buy_price:
                    win_count += 1
                else:
                    loss_count += 1
            
            # 현재 포지션 정보
            last_trade = session.query(TradeHistory).filter(
                TradeHistory.symbol == symbol, TradeHistory.status == "FILLED"
            ).order_by(desc(TradeHistory.timestamp)).first()
            
            current_position = "FLAT"
            entry_price = None
            pos_pnl_str = ""
            pos_duration_str = ""
            
            if last_trade and last_trade.side == "BUY":
                current_position = "LONG"
                entry_price = Decimal(str(last_trade.price))
                pos_return = (current_close - entry_price) / entry_price * 100
                pos_pnl_str = f" ({pos_return:+.2f}%)"
                
                # 진입 시간 경과
                entry_ts = last_trade.timestamp
                if entry_ts.tzinfo is None:
                    entry_ts = entry_ts.replace(tzinfo=timezone.utc)
                else:
                    entry_ts = entry_ts.astimezone(timezone.utc)
                minutes_held = (datetime.now(timezone.utc) - entry_ts).total_seconds() / 60.0
                pos_duration_str = f" ({minutes_held:.0f}분 보유)"
                
            pos_emoji = "🟢 LONG" if current_position == "LONG" else "⚪ FLAT"
            
            # 오늘 하루의 주문 시도 수 집계
            total_count = len(today_trades)
            filled_count = sum(1 for t in today_trades if t.status == "FILLED")
            rejected_count = sum(1 for t in today_trades if t.status == "REJECTED")
            failed_count = sum(1 for t in today_trades if t.status == "FAILED")
            
            fill_rate_str = f"{(filled_count / total_count * 100):.1f}%" if total_count > 0 else "N/A"
            
            evaluated_total = win_count + loss_count
            win_rate_str = f"{(win_count / evaluated_total * 100):.1f}%" if evaluated_total > 0 else "N/A"
            
            if realized_pnl > Decimal("0"):
                pnl_emoji = "📈"
                pnl_sign = "+"
            elif realized_pnl < Decimal("0"):
                pnl_emoji = "📉"
                pnl_sign = ""
            else:
                pnl_emoji = "➖"
                pnl_sign = ""
                
            report_time_kst = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")
            
            report_lines = [
                "📊 <b>[QuantFlow 일일 결산 리포트]</b>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>집계 일자:</b> <code>{now_kst.strftime('%Y-%m-%d')} (KST)</code>",
                "",
                "💰 <b>실시간 자산 총액</b>",
                "────────────────────",
                f"• <b>가용 USDT:</b> <code>${float(usdt_balance):,.2f} USDT</code>",
                f"• <b>보유 BTC:</b> <code>{float(btc_balance):.6f} BTC</code>",
                f"• <b>총 자산 가치:</b> <code>${float(total_assets):,.2f} USDT</code>",
                "",
                "📦 <b>유지 포지션 현황</b>",
                "────────────────────",
                f"• <b>상태:</b> <b>{pos_emoji}</b>{pos_pnl_str}",
                f"• <b>평단가:</b> <code>${float(entry_price or 0):,.2f} USDT</code>" if entry_price else "• <b>평단가:</b> <code>-</code>",
                f"• <b>경과 시간:</b> <code>{pos_duration_str or '-'}</code>" if entry_price else "• <b>경과 시간:</b> <code>-</code>",
                "",
                "📋 <b>오늘 하루 매매 요약</b>",
                "────────────────────",
                f"• <b>총 주문 시도:</b> <code>{total_count}건</code>",
                f"• <b>체결 완료(FILLED):</b> <code>{filled_count}건</code>",
                f"• <b>거부(REJECTED):</b> <code>{rejected_count}건</code>",
                f"• <b>실패(FAILED):</b> <code>{failed_count}건</code>",
                f"• <b>체결 성공률:</b> <code>{fill_rate_str}</code>",
                "",
                "🎯 <b>실현 손익 및 승률</b>",
                "────────────────────",
                f"• {pnl_emoji} <b>금일 실현 손익:</b> <code>{pnl_sign}{realized_pnl:,.2f} USDT</code>",
                f"• <b>익절(Win):</b> <code>{win_count}건</code> / <b>손절(Loss):</b> <code>{loss_count}건</code>",
                f"• <b>승률:</b> <code>{win_rate_str}</code>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>리포트 생성:</b> <code>{report_time_kst}</code>",
            ]
            
            notifier.send_message("\n".join(report_lines))
            logger.info(f"📊 [generate_daily_report_task] 리포트 전송 완료 — PnL: {pnl_sign}{realized_pnl:,.2f} USDT")
            
            return {
                "status": "report_sent",
                "total_trades": total_count,
                "filled_trades": filled_count,
                "realized_pnl": str(realized_pnl),
                "win_rate": win_rate_str,
            }
            
    except Exception as exc:
        logger.error(f"❌ [generate_daily_report_task] 리포트 생성 중 예외 발생: {exc}", exc_info=True)
        return {"status": "report_failed", "error": str(exc)}