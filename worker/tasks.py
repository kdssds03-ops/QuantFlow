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
STOP_LOSS_THRESHOLD = Decimal("-0.0100") # -1.0% 손절 방패 임계치
CONFIDENCE_THRESHOLD = 0.65             # 스나이퍼 진입 확신도 65% Filter

# ── [Phase 3] 주문 집행 파이프라인 설정 상수 ─────────────────────────────
_ORDER_FILL_POLL_INTERVAL_SEC = 1   # 체결 폴링 간격 (초)
_ORDER_FILL_POLL_MAX_SEC = 5        # 체결 폴링 최대 대기 시간 (초)
_ORDER_RETRY_MAX_ATTEMPTS = 3       # 네트워크 재시도 최대 횟수
_ORDER_RETRY_WAIT_MIN_SEC = 1       # Exponential Backoff 최소 대기 (초)
_ORDER_RETRY_WAIT_MAX_SEC = 8       # Exponential Backoff 최대 대기 (초)


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

        # ── NULL 지표 레코드만 선별하여 upsert ──────────────────────────────
        upsert_count = 0
        for i, r in enumerate(rows):
            row_feat = df.iloc[i]
            sma_20_val   = _to_dec(row_feat.get("sma_20"))
            rsi_14_val   = _to_dec(row_feat.get("rsi_14"))
            bb_upper_val = _to_dec(row_feat.get("bb_upper"))
            bb_lower_val = _to_dec(row_feat.get("bb_lower"))

            # 지표가 NULL인 행만 upsert (정상 행 불필요한 재기록 방지)
            if r.rsi_14 is None or r.bb_upper is None or r.bb_lower is None:
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
                ).on_conflict_do_update(
                    constraint="uq_market_data_ts_symbol",
                    set_={
                        "sma_20":   pg_insert(MarketData).excluded.sma_20,
                        "rsi_14":   pg_insert(MarketData).excluded.rsi_14,
                        "bb_upper": pg_insert(MarketData).excluded.bb_upper,
                        "bb_lower": pg_insert(MarketData).excluded.bb_lower,
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

        last = df.iloc[-1]
        # ── [Timezone Fix] UTC timezone-aware datetime 강제 보장 ─────────────
        candle_dt = _ensure_utc(int(last["timestamp_ms"]))

        sma_20   = _to_dec(last.get("sma_20"))
        rsi_14   = _to_dec(last.get("rsi_14"))
        bb_upper = _to_dec(last.get("bb_upper"))
        bb_lower = _to_dec(last.get("bb_lower"))

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
        )
        stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_market_data_ts_symbol",
            set_={
                "sma_20":   insert_stmt.excluded.sma_20,
                "rsi_14":   insert_stmt.excluded.rsi_14,
                "bb_upper": insert_stmt.excluded.bb_upper,
                "bb_lower": insert_stmt.excluded.bb_lower,
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
        exchange = get_exchange()
        
        # 1. 실시간 자산 잔고 트래킹 (매매 수량 동적 계산 및 알림 연동용)
        try:
            balance_info = exchange.fetch_balance()
            usdt_balance = Decimal(str(balance_info["total"].get("USDT", 0.0)))
            usdt_free = Decimal(str(balance_info["free"].get("USDT", 0.0)))
            btc_free = Decimal(str(balance_info["free"].get("BTC", 0.0)))
        except Exception as e:
            logger.error(f"❌ 실시간 지갑 잔고 조회 실패: {e} -> 매매 파이프라인 스킵")
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
                # → DB 최근 500봉으로 지표를 재계산하여 즉시 UPDATE 영구 저장.
                _hist_rows = (
                    session.query(MarketData)
                    .filter(MarketData.symbol == symbol)
                    .order_by(desc(MarketData.timestamp))
                    .limit(500)
                    .all()
                )
                if len(_hist_rows) >= 20:  # sma_20 최소 요구 봉 수
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
                    _last_feat = _df_hist.iloc[-1]
                    _patched_rsi    = _to_dec(_last_feat.get("rsi_14"))
                    _patched_upper  = _to_dec(_last_feat.get("bb_upper"))
                    _patched_lower  = _to_dec(_last_feat.get("bb_lower"))
                    _patched_sma    = _to_dec(_last_feat.get("sma_20"))
                    if _patched_rsi is not None:
                        # ── [동시성 격리] 지표 패치 전용 독립 세션으로 UPDATE ────────
                        # 공유 세션(session)에서 commit()을 호출하면 fetch_market_data_task
                        # 등 동시 실행 중인 다른 태스크의 트랜잭션 상태를 오염시켜
                        # PGRES_TUPLES_OK 충돌이 발생함.
                        # → 패치 전용 독립 세션을 별도 생성하여 완전 격리 처리.
                        #   메인 세션(session)의 트랜잭션은 일절 건드리지 않음.
                        _patch_session = _SyncSession()
                        try:
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
                            _patch_session.rollback()
                            logger.warning(
                                "⚠️ [지표 패치] DB UPDATE 실패 (id=%s): %s",
                                latest.id, _patch_db_exc,
                            )
                        finally:
                            _patch_session.close()
                        # 메모리상 객체도 동기화 (현재 턴 분석 정상 진행)
                        latest.rsi_14   = _patched_rsi
                        latest.bb_upper = _patched_upper
                        latest.bb_lower = _patched_lower
                        latest.sma_20   = _patched_sma
                    else:
                        logger.warning(
                            "⚠️ [지표 패치] 재계산 후에도 NaN — 데이터 %d봉 부족, 분석 스킵",
                            len(_hist_rows),
                        )
                        return {"status": "insufficient_indicator_data"}
                else:
                    logger.warning(
                        "⚠️ [지표 패치] DB 데이터 부족(%d봉 < 20봉) — 분석 스킵",
                        len(_hist_rows),
                    )
                    return {"status": "insufficient_data"}

            current_close = Decimal(str(latest.close))

            # 3. 가장 최근 체결 주문 기반 실시간 포지션(롱/플랫) 상태 분석
            last_trade = session.query(TradeHistory).filter(TradeHistory.symbol == symbol, TradeHistory.status == "FILLED").order_by(desc(TradeHistory.timestamp)).first()
            
            current_position = "FLAT"
            entry_price = None
            if last_trade and last_trade.side == "BUY":
                current_position = "LONG"
                entry_price = Decimal(str(last_trade.price))

            # 4. 🛡️ [손절 방패 (우선순위 1위)] 리스크 관리 작동 검사
            if current_position == "LONG" and entry_price:
                price_return = (current_close - entry_price) / entry_price
                if price_return <= STOP_LOSS_THRESHOLD:
                    logger.warning(f"🚨 [손절 방패 가동] 평단가: {entry_price} -> 현재가: {current_close} ({price_return*100:.2f}%)")
                    
                    # [동적 수량 계산 - 손절 청산]: BTC 가용 잔고 전량 청산
                    calculated_amount = btc_free
                    
                    # 방어적 검증 (Short-circuit): 최소 주문 수량 미만 검사
                    if calculated_amount <= Decimal("0") or calculated_amount < MIN_ORDER_BTC:
                        logger.warning(
                            f"⏸️  [손절 방패] 계산된 청산 수량이 부족하여 주문 생략: "
                            f"BTC 잔고={calculated_amount}, 최소 필요={MIN_ORDER_BTC}"
                        )
                        return {"status": "insufficient_calculated_amount"}

                    # 🚀 [Phase 3] 프로덕션 등급 주문 집행 파이프라인 호출 (손절 방패)
                    sl_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="SELL",
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

            # 5. 🎯 ML/Rule-based 스나이퍼 추론 및 확신도 필터링
            if hasattr(_predictor, "predict_with_confidence"):
                action, confidence = _predictor.predict_with_confidence(latest)
            else:
                # 레거시 폴백 대응용 구조
                action = _predictor.predict(latest)
                confidence = CONFIDENCE_THRESHOLD if action != "HOLD" else 0.0

            if action == "HOLD" or confidence < CONFIDENCE_THRESHOLD:
                return {"status": "hold_or_low_confidence", "confidence": confidence}

            # 6. ⏳ 멱등성 보장을 위한 5분 쿨다운 검사
            cooldown_cutoff = datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES)
            recent_same_side = session.query(TradeHistory).filter(
                TradeHistory.symbol == symbol, TradeHistory.side == action, TradeHistory.timestamp >= cooldown_cutoff
            ).first()

            if recent_same_side is not None:
                logger.warning(f"⏳ 쿨다운 필터 발동: {action} 주문 생성 스킵")
                return {"status": "cooldown_skipped"}

            # 7. ⚡ 주문 분기점 정의 (신규 진입 vs 역시그널 청산) 및 동적 주문 수량 계산
            trigger_type = "SNIPER_ENTRY"
            if current_position == "LONG" and action == "SELL":
                trigger_type = "REVERSE_SWITCH_EXIT"  # '창과 방패'의 익절 조화

            # 동적 주문 수량 연산 (USDT 10% vs BTC 100%)
            if action == "BUY":
                # 가용 USDT의 10%만큼 매수 수량 계산
                target_usdt = usdt_free * Decimal("0.1")
                calculated_amount = target_usdt / current_close
                logger.info(f"💰 [자산 배분 - BUY] 가용 USDT: {usdt_free} -> 진입 목표: {target_usdt} USDT -> 계산 수량: {calculated_amount} BTC")
            elif action == "SELL":
                # 가용 BTC 전량 청산
                calculated_amount = btc_free
                logger.info(f"💰 [자산 배분 - SELL] 가용 BTC 전량 청산: {calculated_amount} BTC")
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
                timestamp=datetime.now(timezone.utc), symbol=symbol, side=action,
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