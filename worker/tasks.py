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

Phase 6 (Zero-I/O Latency & Numpy 고속화):
  - ohlcv_stream.OhlcvStreamManager 연동: WebSocket 인메모리 큐 우선 참조
    · REST fetch_ohlcv() 폴링 제거 → HTTP 왕복 50~300ms 지연 제거
    · deque(maxlen=500) 스냅샷 조회 → 0ms 근접 데이터 접근
    · 큐 미달 시 REST Fallback 자동 수행
  - ta 라이브러리 완전 제거: indicators.compute_all_features()로
    ATR/MACD 연산 일원화 (순수 Numpy Wilder 스무딩 기반)
  - _get_lot_size_step() 헬퍼: 거래소별 Lot Size를 동적 연동하여
    주문 수량 Decimal.quantize() 강제화 → InvalidOrder 원천 차단
"""

import asyncio
import json
import logging
import math
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import TypedDict, Optional

import ccxt
import pandas as pd
import redis as _sync_redis_lib  # 동기 Redis 클라이언트 (분산 락 전용)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)

from sqlalchemy import create_engine, desc
from sqlalchemy.future import select as sa_select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ── 비동기 세션 팩토리 (core.database 비동기 엔진 기반) ──────────────────────
from core.database import async_session as _async_session_factory

from worker.celery_app import celery_app
from core.config import get_settings
from core.exchange import get_exchange
from core.time_sync import check_ntp_drift
from app.models.models import MarketData, TradeHistory

# [Phase 6] WebSocket 인메모리 큐 레이어 연동
# 이 import는 모듈 로드 시 WebSocket 스트림매니저 싱글턴이 인스턴스화됩니다.
# ohlcv_stream_manager.start() — 필요 시 명시적으로 호출하여 WebSocket 연결을 시작합니다.
try:
    from worker.ohlcv_stream import ohlcv_stream_manager as _ohlcv_stream
    if _ohlcv_stream is not None:
        _ohlcv_stream.start(["BTC/USDT"])
    logging.getLogger(__name__).info("⚡ [OhlcvStream] 인메모리 큐 레이어 로드 및 스트림 시작 완료")
except ImportError:
    _ohlcv_stream = None
    logging.getLogger(__name__).warning(
        "⚠️ [OhlcvStream] ohlcv_stream 모듈 미설치 — REST Fallback 모드로 가동"
    )

# [관심사 분리] 실시간 텔레그램 알림 모듈 결합
from core.notifier import notifier


logger = logging.getLogger(__name__)
settings = get_settings()

# ── [최적화 ②] 모듈 레벨 Redis 싱글턴 — 매 호출마다 연결을 새로 생성하지 않음 ──────
# analyze_and_trade() 내 분산락·피라미딩 카운터·리셋에서 이 클라이언트를 공유합니다.
try:
    _r_sync = _sync_redis_lib.Redis.from_url(
        settings.redis_url, decode_responses=True, socket_connect_timeout=2
    )
    _r_sync.ping()  # 연결 사전 검증
    logger.info("✅ [Redis 싱글턴] 모듈 레벨 Redis 클라이언트 초기화 완료")
except Exception as _r_sync_init_exc:
    _r_sync = None
    logger.warning("⚠️ [Redis 싱글턴] 초기화 실패 → 각 호출부에서 개별 생성으로 폴백: %s", _r_sync_init_exc)

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
from worker.predictor import (
    BasePredictor, RuleBasedPredictor, MLPredictor, TrendFollowingPredictor,
)

def _resolve_predictor() -> BasePredictor:
    """
    환경 변수 'PREDICTOR_TYPE' 설정값에 따라 다형성이 보장된 적절한 매매 예측 엔진을 동적으로 주입합니다.

    지원 값:
      RULE  — 볼린저+RSI 평균회귀 (기본)
      ML    — LightGBM 추론 (model.pkl 필요)
      TREND — 4h EMA 교차 추세추종 (백테스트 검증된 양방향 엣지)
              · EMA 기간은 TREND_EMA_FAST/TREND_EMA_SLOW, TF는 TREND_TF_MIN(분) 환경변수로 조정
              · 켜지면 평균회귀용 가드(타임아웃/하드TP/타이트SL)가 자동 비활성화됨 (아래 _TREND_MODE)
    """
    predictor_type = os.getenv("PREDICTOR_TYPE", "RULE").strip().upper()

    if predictor_type == "ML":
        logger.info("🤖 [Dependency Injection] 'ML' 예측 엔진 감지 -> MLPredictor 동적 주입 완료")
        return MLPredictor(session_factory=_SyncSession, confidence_threshold=CONFIDENCE_THRESHOLD)

    if predictor_type == "TREND":
        logger.info("📈 [Dependency Injection] 'TREND' 예측 엔진 감지 -> TrendFollowingPredictor 동적 주입 완료")
        return TrendFollowingPredictor(
            session_factory=_SyncSession,
            ema_fast=int(os.getenv("TREND_EMA_FAST", "30")),
            ema_slow=int(os.getenv("TREND_EMA_SLOW", "60")),
            timeframe_minutes=int(os.getenv("TREND_TF_MIN", "240")),
        )

    # "RULE" 이거나 설정되지 않은(None, 공백) 경우 RuleBasedPredictor로 안전하게 폴백
    logger.info("🟢 [Dependency Injection] 'RULE' 예측 엔진 감지 (또는 Fallback) -> RuleBasedPredictor 동적 주입 완료")
    return RuleBasedPredictor()

# ⚠️ CONFIDENCE_THRESHOLD를 _resolve_predictor() 호출보다 먼저 정의해야 한다.
#    MLPredictor 분기가 이 상수를 참조하므로, 아래 "[Strict Rules]" 섹션이 아닌
#    여기서 선정의하여 PREDICTOR_TYPE=ML 기동 시 NameError를 방지한다.
CONFIDENCE_THRESHOLD = 0.65             # 스나이퍼 진입 확신도 65% Filter (동적 하향 적용 전 기본값)

# Celery 워커 최초 메모리 가동 시 1회 싱글턴 초기화 수행
_predictor: BasePredictor = _resolve_predictor()

# ── [웰컴 알림] Redis 기반 글로벌 멱등성 가드 ────────────────
# 다중 컨테이너 격리 환경(worker, listener, api, beat 등)에서도 
# 글로벌 공유 인프라인 Redis 키를 활용하여 딱 1회만 알림이 전송되도록 가드합니다.
_WELCOME_REDIS_KEY = "quantflow:welcome_sent_flag"
_welcome_already_sent = False

try:
    # 싱글턴 클라이언트가 준비되어 있으면 재사용, 아니면 임시 연결
    _r_welcome = _r_sync if _r_sync is not None else _sync_redis_lib.Redis.from_url(
        settings.redis_url, decode_responses=True, socket_connect_timeout=2
    )
    # Redis의 set(..., nx=True) 원자적 연산을 사용하여 단 하나의 컨테이너만 발송 성공하도록 보장
    # 24시간(86400초) 동안 웰컴 키 유지하여 봇이 자주 재시작할 때 스팸 방지
    _welcome_already_sent = not _r_welcome.set(_WELCOME_REDIS_KEY, "sent", ex=86400, nx=True)
except Exception as _welcome_redis_exc:
    # 만약 Redis가 점검 중이거나 접속 실패하면, 컨테이너별 로컬 임시파일 시스템으로 자동 폴백
    logger.warning("⚠️ [웰컴 알림] Redis 플래그 확인 실패 → 로컬 파일 시스템으로 폴백합니다: %s", _welcome_redis_exc)
    import tempfile
    _WELCOME_FLAG_FILE = os.path.join(tempfile.gettempdir(), "quantflow_welcome_sent")
    _welcome_already_sent = os.path.exists(_WELCOME_FLAG_FILE)

if not _welcome_already_sent:
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
        # Redis 연결 문제로 로컬 파일로 폴백한 경우 파일 생성
        try:
            if 'tempfile' in locals() or '_WELCOME_FLAG_FILE' in locals():
                with open(_WELCOME_FLAG_FILE, "w") as _f:
                    from datetime import datetime as _dt
                    _f.write(f"sent at {_dt.now().isoformat()}")
        except Exception:
            pass
        logger.info("🚩 [웰컴 알림] 글로벌 가동 완료 메시지 전송 성공")
    except Exception as _e:
        # 전송 실패 시 Redis 키 삭제하여 재시도 가능하게 조치
        logger.error("❌ [웰컴 알림] 발송 실패 (글로벌 락 해제, 다음 기동 시 재시도): %s", _e)
        try:
            _r_welcome.delete(_WELCOME_REDIS_KEY)
        except Exception:
            pass
else:
    logger.debug("🚩 [웰컴 알림] 글로벌/로컬 플래그 감지 — 중복 발송 차단")

# ── [Strict Rules] 정밀도 유지 및 리스크 관리 임계치 설정 ────────────────
TRADE_AMOUNT_BTC = Decimal("0.001")     # 일반 float(0.001)에서 Decimal 구조로 정형화
MIN_ORDER_BTC = Decimal("0.0001")       # 바이낸스 등 거래소 시장가 주문 최저 한계선 (BTC)
COOLDOWN_MINUTES = 5
STOP_LOSS_THRESHOLD = Decimal("-0.0100") # -1.0% 소프트 손절 방패 (지표 기반)
# CONFIDENCE_THRESHOLD는 _resolve_predictor() 의존성 때문에 모듈 상단에서 선정의됨 (위 참조)

# ── 진입 사이징 (위험/수익 손잡이) — .env RISK_FACTOR로 조절 ───────────────
# 신규 진입 1회당 가용 마진의 이 비율을 투입. 기본 0.10(10%).
# 0 < x <= 1 범위로 클램프하여 잘못된 설정으로 인한 과도 레버리지/0 주문을 방지.
RISK_FACTOR = Decimal(str(settings.risk_factor))
if RISK_FACTOR <= Decimal("0") or RISK_FACTOR > Decimal("1"):
    logger.warning(
        "⚠️ [RISK_FACTOR] 설정값 %s 이 허용범위(0~1)를 벗어남 → 0.10으로 보정", RISK_FACTOR
    )
    RISK_FACTOR = Decimal("0.10")
logger.info("⚖️ [RISK_FACTOR] 진입 사이징 = 가용 마진의 %.0f%%", float(RISK_FACTOR) * 100)

# ── [Phase 6] 불타기 엔진 설정 ─────────────────────────────────────────────
# 추세장 한정 피라미딩 가드 완화: 기존 0.5% → 0.2%
PRICE_BUFFER_PCT_TREND   = Decimal("0.002")  # 추세장 피라미딩 가드 (0.2%)
PRICE_BUFFER_PCT_DEFAULT = Decimal("0.005")  # 보수 기본값 유지 (0.5%)
# 불타기 발동 조건: 현재 LONG 포지션이 이 ROI 이상 수익권일 때
PYRAMID_PROFIT_THRESHOLD = Decimal("0.005")  # +0.5% 수익권 진입 조건
# 불타기 최대 횟수 (Redis 카운터 기반 추적)
PYRAMID_MAX_ADDS = 2
# 불타기 추격 매수 비율: 기존 포지션 수량의 50%
PYRAMID_ADD_RATIO = Decimal("0.50")
# 불타기 Redis 카운터 키 네임스페이스
_PYRAMID_COUNT_KEY_PREFIX = "quantflow:pyramid_count:"
_PYRAMID_COUNT_TTL_SEC    = 3600  # 1시간 후 자동 만료 (포지션 정리 후 리셋 보장)

# ── [결함 #1] 중복 주문 방지용 Redis 분산 락 설정 ─────────────────────────
# 동일 심볼에 대한 analyze_and_trade 태스크가 30초 이내 재진입할 경우 즉시 Skip.
# Redis SETNX + EXPIRE 조합: 락 획득에 성공한 프로세스만 매매 로직을 실행.
_ORDER_DEDUP_LOCK_TTL_SEC = 30  # 심볼별 락 유효 시간 (초) — 이 시간 내 재트리거 무시
_ORDER_DEDUP_LOCK_PREFIX  = "quantflow:order_lock:"  # Redis 키 네임스페이스

# ── [v5.0] 시장 국면 판독기(Regime Classifier) 임계치 ──────────────────────
# 추세 국면(TREND) 판정: 아래 두 조건 모두 충족 시
#   1) ADX(14) ≥ ADX_TREND_THRESHOLD (방향성 강도 기준)
#   2) 현재 ATR ≥ 최근 ATR_LOOKBACK_CANDLES 봉 평균 ATR × ATR_EXPANSION_RATIO (변동성 확장 기준)
# 하나라도 미충족 시 즉시 횡보 국면(CHOP) 판정.
ADX_TREND_THRESHOLD    = 25.0              # ADX 추세 강도 판정 임계치
ATR_EXPANSION_RATIO    = Decimal("1.5")   # ATR 변동성 확장 배율 (현재 ATR / 평균 ATR)
ATR_LOOKBACK_CANDLES   = 240              # ATR 기준 평균 산출 기간 (1분봉 240개 = 4시간)

# ── [v5.0] 횡보 국면 피라미딩 강제 잠금 파라미터 ─────────────────────────
PYRAMID_MAX_ADDS_CHOP  = 0               # 횡보 국면 시 불타기 횟수 강제 0 (전면 봉쇄)
PRICE_BUFFER_PCT_CHOP  = Decimal("0.005") # 횡보 국면 피라미딩 가드 (0.5% 보수 기준)

# ── [v5.0] 인프라 레벨 리스크 방화벽 임계치 ──────────────────────────────
# 자본 방화벽: 단일 심볼 누적 증거금이 가용 마진의 MAX_CAPITAL_PCT 초과 시 주문 거부
MAX_CAPITAL_PER_SYMBOL_PCT = Decimal("0.20")  # 20% 한도
# 슬리페이지 가드: WebSocket 최신가 vs 호가창 스프레드 괴리 허용 한도
SLIPPAGE_GUARD_PCT         = Decimal("0.001") # 0.1% 초과 시 주문 REJECTED

# ── [8차 확장] 하드 TP/SL + 타임아웃 안전장치 임계치 ────────────────────
HARD_SL_THRESHOLD   = Decimal("-0.0150") # -1.5% 하드 손절 컷 (무조건 강제 청산)
HARD_TP_THRESHOLD   = Decimal("0.0300")  # +3.0% 하드 익절 타겟 (무조건 강제 수확)
MAX_POSITION_MINUTES = 240               # 최대 포지션 보유 시간 (240분 = 4시간) [v9.2: 3h→4h]

# ── [v9.2] 익절 보존형 트레일링 스탑 가드 임계치 ─────────────────────────
# Peak ROI가 TRAILING_STOP_ACTIVATION_ROI 이상을 한 번이라도 터치한 뒤,
# 고점 대비 수익률이 TRAILING_STOP_DRAWDOWN 이상 반납되면 즉시 전량 익절 청산.
TRAILING_STOP_ACTIVATION_ROI = Decimal("0.1500")  # +15.0% — 트레일링 가드 활성화 기준선
TRAILING_STOP_DRAWDOWN       = Decimal("0.0500")  # -5.0%  — 고점 대비 반납 허용 한계

# ══════════════════════════════════════════════════════════════════════════
# 📈 [추세추종 모드] PREDICTOR_TYPE=TREND 시 평균회귀용 가드 자동 비활성화
# ══════════════════════════════════════════════════════════════════════════
# 위 가드들(타임아웃 240분·하드TP +3%·타이트SL -1%)은 1m 평균회귀에 맞춰진 값으로,
# 추세추종의 '승자를 길게 태우고 추세전환 시그널로 청산'하는 엣지를 파괴한다.
# (특히 240분 타임아웃은 수일~수주 보유하는 추세 포지션을 4시간마다 강제 청산시킴)
# → TREND 모드에서는 이 가드들을 비활성화하고, 청산은 반대 시그널(스톱앤리버스)이
#   담당하게 하며, 광범위 재난 스톱(-12%)만 안전망으로 남긴다.
# 이 재바인딩은 모듈 전역 상수를 덮어쓰므로, 전역을 읽는 analyze_and_trade가
# 함수 본문 수정 없이 그대로 새 값을 사용한다. (RULE/ML 모드에서는 전혀 영향 없음)
_TREND_MODE = isinstance(_predictor, TrendFollowingPredictor)
if _TREND_MODE:
    STOP_LOSS_THRESHOLD          = Decimal("-0.12")   # 소프트SL: -1% → -12% (재난 방지용으로만)
    HARD_SL_THRESHOLD            = Decimal("-0.15")   # 하드SL: -1.5% → -15% (극단 안전망)
    HARD_TP_THRESHOLD            = Decimal("100")     # 하드TP: +3% → 사실상 비활성 (익절은 추세전환 담당)
    MAX_POSITION_MINUTES         = 10**9              # 타임아웃: 240분 → 사실상 비활성
    TRAILING_STOP_ACTIVATION_ROI = Decimal("100")     # 트레일링도 비활성 (추세전환 시그널 우선)
    logger.warning(
        "📈 [TREND 모드] 평균회귀용 가드 자동 비활성화 — "
        "타임아웃/하드TP/타이트SL/트레일링 OFF, 재난 스톱(-15%)만 유지. "
        "청산은 4h 추세전환 시그널(스톱앤리버스)이 담당."
    )

# ── [Phase 3] 주문 집행 파이프라인 설정 상수 ─────────────────────────────
_ORDER_FILL_POLL_INTERVAL_SEC = 1   # 체결 폴링 간격 (초)
_ORDER_FILL_POLL_MAX_SEC = 5        # 체결 폴링 최대 대기 시간 (초)
_ORDER_RETRY_MAX_ATTEMPTS = 3       # 네트워크 재시도 최대 횟수
_ORDER_RETRY_WAIT_MIN_SEC = 1       # Exponential Backoff 최소 대기 (초)
_ORDER_RETRY_WAIT_MAX_SEC = 8       # Exponential Backoff 최대 대기 (초)
_BALANCE_RETRY_MAX = 3              # fetch_balance 재시도 최대 횟수
_BALANCE_RETRY_WAIT_SEC = 1         # fetch_balance 재시도 대기 (초)

# ── [v9.2] Peak ROI 인메모리 추적 레지스터 ───────────────────────────────
# 포지션별 최고 수익률(Peak ROI)을 워커 프로세스 메모리에 영속 유지.
# key: symbol (str, 정규화된 포맷 — 슬래시/하이픈 제거, 예: "BTCUSDT"), value: Decimal
# 포지션 진입(SELL) 시 0으로 초기화, 청산 완료 시 삭제.
# Celery prefork 환경에서 단일 프로세스 기준이므로, 워커 재기동 시 리셋됨.
# (재기동 후 복구가 필요하면 Redis 연동으로 확장 가능)
_peak_roi_register: dict[str, Decimal] = {}


def _normalize_symbol(symbol: str) -> str:
    """
    🔑 [심볼 정규화 헬퍼] 바이낸스 raw 포맷("BTCUSDT")과 CCXT 포맷("BTC/USDT") 간의
    KeyMismatch를 방지하기 위해 슬래시(/) 및 하이픈(-)을 제거한 통합 키로 정규화합니다.

    예시:
        "BTC/USDT"  -> "BTCUSDT"
        "BTC-USDT"  -> "BTCUSDT"
        "BTCUSDT"   -> "BTCUSDT"  (이미 정규화됨, 그대로 반환)
    """
    return symbol.replace("/", "").replace("-", "").upper()


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
    """float/Decimal NaN 및 None, pd.NA를 안전하게 None으로 변환 후 Decimal 반환."""
    if val is None:
        return None
    if pd.isna(val):
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

# ══════════════════════════════════════════════════════════════════════════
# 🧭 [v5.0] 시장 국면 판독기(Regime Classifier) — 인프라 레벨 최상단 컴포넌트
# ══════════════════════════════════════════════════════════════════════════

def _classify_market_regime(
    df: pd.DataFrame,
    adx_threshold: float = 25.0,
    atr_ratio: Decimal = Decimal("1.5"),
    atr_lookback: int = 240,
) -> tuple:
    """
    시장 국면 판독기 (Regime Classifier).

    [판정 로직 - AND 조건]
    추세 국면(TREND): 아래 두 조건을 모두 충족
      1. ADX(14) 최신값 >= adx_threshold (기본 25.0)
      2. 현재 ATR(14) >= 최근 atr_lookback 봉 평균 ATR x atr_ratio (기본 1.5배)
    횡보 국면(CHOP): 위 조건 중 하나라도 미충족
    FLAT_HOLD 모드: ADX/ATR/RSI/BB 지표 중 하나라도 NaN -> 즉시 보수 모드

    Args:
        df           : compute_all_features()가 적용된 OHLCV DataFrame
        adx_threshold: ADX 추세 강도 임계치 (기본 25.0)
        atr_ratio    : ATR 변동성 확장 배율 임계치 (기본 1.5)
        atr_lookback : ATR 평균 산출 기간 (기본 240봉)

    Returns:
        tuple("TREND" | "CHOP" | "FLAT_HOLD", metadata_dict)
    """
    meta: dict = {
        "adx_val": None,
        "atr_current": None,
        "atr_avg": None,
        "atr_ratio_actual": None,
        "reason": "",
    }

    try:
        # -- [Step 1] 지표 NaN Failsafe 검증 ----------------------------------------
        # 판별 지표 어느 하나라도 NaN/None이면 즉시 FLAT_HOLD 반환.
        # pd.isna()는 float NaN / numpy NaN / None / pd.NA 모두 커버.
        last = df.iloc[-1]

        _raw_adx = last.get("adx_14")
        _raw_atr = last.get("atr_14")
        _raw_rsi = last.get("rsi_14")
        _raw_bbu = last.get("bb_upper")
        _raw_bbl = last.get("bb_lower")

        for _field_name, _field_val in [
            ("adx_14",   _raw_adx),
            ("atr_14",   _raw_atr),
            ("rsi_14",   _raw_rsi),
            ("bb_upper", _raw_bbu),
            ("bb_lower", _raw_bbl),
        ]:
            if _field_val is None:
                meta["reason"] = f"NaN Failsafe: {_field_name} is None"
                logger.warning(
                    "⚠️ [REGIME] NaN Failsafe 발동 — %s is None → FLAT_HOLD 모드 전환",
                    _field_name,
                )
                return ("FLAT_HOLD", meta)
            try:
                if pd.isna(_field_val):
                    meta["reason"] = f"NaN Failsafe: {_field_name} is NaN"
                    logger.warning(
                        "⚠️ [REGIME] NaN Failsafe 발동 — %s is NaN → FLAT_HOLD 모드 전환",
                        _field_name,
                    )
                    return ("FLAT_HOLD", meta)
            except (TypeError, ValueError):
                pass

        # -- [Step 2] 지표값 안전 추출 ------------------------------------------------
        adx_val = float(_raw_adx)
        atr_current = Decimal(str(_raw_atr))
        meta["adx_val"] = adx_val
        meta["atr_current"] = float(atr_current)

        # -- [Step 3] ATR 기준 평균 산출 (최근 atr_lookback 봉) ----------------------
        atr_series = df["atr_14"].dropna()
        if len(atr_series) < 2:
            meta["reason"] = "ATR 데이터 부족 (유효 봉 < 2) → FLAT_HOLD"
            logger.warning("⚠️ [REGIME] ATR 유효 데이터 부족 → FLAT_HOLD 모드")
            return ("FLAT_HOLD", meta)

        recent_atr = atr_series.tail(atr_lookback)
        atr_avg = Decimal(str(float(recent_atr.mean())))
        meta["atr_avg"] = float(atr_avg)

        # -- [Step 4] 0 나눗셈 방어 ---------------------------------------------------
        if atr_avg <= Decimal("0"):
            meta["reason"] = "ATR 평균이 0 이하 → FLAT_HOLD (0 나눗셈 방지)"
            logger.warning("⚠️ [REGIME] ATR 평균 = 0 → FLAT_HOLD 모드")
            return ("FLAT_HOLD", meta)

        # -- [Step 5] 추세/횡보 판정 (AND 조건) ---------------------------------------
        atr_ratio_actual = atr_current / atr_avg
        meta["atr_ratio_actual"] = float(atr_ratio_actual)

        cond_adx = adx_val >= adx_threshold
        cond_atr = atr_ratio_actual >= atr_ratio

        if cond_adx and cond_atr:
            meta["reason"] = (
                f"TREND: ADX={adx_val:.2f}>={adx_threshold}, "
                f"ATR배율={float(atr_ratio_actual):.3f}>={float(atr_ratio)}"
            )
            logger.info(
                "📈 [REGIME] 추세 국면(TREND) 판정 — ADX=%.2f (>=%.0f), "
                "ATR배율=%.3f (>=%.1f) → 피라미딩 공격성 유지",
                adx_val, adx_threshold, float(atr_ratio_actual), float(atr_ratio),
            )
            return ("TREND", meta)
        else:
            miss = []
            if not cond_adx:
                miss.append(f"ADX={adx_val:.2f}<{adx_threshold}")
            if not cond_atr:
                miss.append(f"ATR배율={float(atr_ratio_actual):.3f}<{float(atr_ratio)}")
            meta["reason"] = "CHOP: " + ", ".join(miss)
            logger.info(
                "🌀 [REGIME] 횡보 국면(CHOP) 판정 — %s → 피라미딩 봉쇄, 버퍼 0.5%% 상향",
                meta["reason"],
            )
            return ("CHOP", meta)

    except Exception as _regime_exc:
        meta["reason"] = f"Regime 판정 중 예외: {_regime_exc}"
        logger.error(
            "❌ [REGIME] 국면 판정 중 예외 발생 → FLAT_HOLD 보수 처리: %s",
            _regime_exc, exc_info=True,
        )
        return ("FLAT_HOLD", meta)


# ══════════════════════════════════════════════════════════════════════════
# 🛡️ [v5.0] 인프라 레벨 리스크 방화벽 — 자본 한도 & 슬리페이지 가드
# ══════════════════════════════════════════════════════════════════════════

def _check_capital_guard(
    symbol: str,
    order_usdt_value: Decimal,
    usdt_free: Decimal,
    max_pct: Decimal = Decimal("0.20"),
) -> bool:
    """
    최대 진입 한도 강제 가드 (자본 방화벽).

    단일 심볼에 할당되는 주문 USDT 환산 증거금이 가용 마진(usdt_free)의
    max_pct(기본 20%)를 초과할 경우 False(주문 거부)를 반환합니다.

    Args:
        symbol           : 거래 심볼 (로그 출력 전용)
        order_usdt_value : 주문 USDT 환산 가치 (가격 x 수량)
        usdt_free        : 선물 지갑 가용 마진 잔고
        max_pct          : 허용 최대 비율 (기본 0.20 = 20%)

    Returns:
        True  — 주문 허용
        False — 주문 거부 (한도 초과)
    """
    try:
        if usdt_free <= Decimal("0"):
            logger.error(
                "🚨 [CAPITAL_GUARD] '%s' 가용 마진이 0 이하 → 주문 즉시 거부: usdt_free=%s",
                symbol, usdt_free,
            )
            return False

        cap_limit = usdt_free * max_pct
        if order_usdt_value > cap_limit:
            logger.error(
                "🚨 [CAPITAL_GUARD] '%s' 주문 한도 초과 → 주문 거부: "
                "주문액=%.2f USDT > 한도(%.0f%% x %.2f USDT = %.2f USDT)",
                symbol,
                float(order_usdt_value),
                float(max_pct) * 100,
                float(usdt_free),
                float(cap_limit),
            )
            return False

        logger.debug(
            "✅ [CAPITAL_GUARD] '%s' 자본 한도 통과: 주문액=%.2f USDT <= 한도=%.2f USDT",
            symbol, float(order_usdt_value), float(cap_limit),
        )
        return True

    except Exception as _cap_exc:
        logger.error(
            "❌ [CAPITAL_GUARD] 자본 한도 검증 중 예외 → 주문 거부 (안전 처리): %s", _cap_exc
        )
        return False


def _check_slippage_guard(
    exchange: ccxt.Exchange,
    symbol: str,
    ref_price: Decimal,
    side: str,
    limit_pct: Decimal = Decimal("0.001"),
) -> bool:
    """
    허용 슬리페이지 가드 인터페이스 (인프라 레벨 Short-circuit).

    시장가 주문 집행 직전, WebSocket 최신 체결가(ref_price) 대비
    실제 호가창의 최우선 호가(BUY→ask / SELL→bid) 스프레드 괴리가
    limit_pct(기본 0.1%)를 초과하면 False(REJECTED 처리 지시)를 반환합니다.

    Args:
        exchange  : ccxt.Exchange 인스턴스
        symbol    : 거래 심볼
        ref_price : WebSocket 최신 체결가 (기준 가격)
        side      : 주문 방향 ("BUY" | "SELL")
        limit_pct : 허용 슬리페이지 비율 (기본 0.001 = 0.1%)

    Returns:
        True  — 슬리페이지 허용 범위 내 → 주문 허용
        False — 슬리페이지 초과 → 주문 REJECTED
    """
    try:
        if ref_price <= Decimal("0"):
            logger.error(
                "🚨 [SLIPPAGE_GUARD] '%s' 기준가(ref_price)가 0 이하 → 주문 거부: ref_price=%s",
                symbol, ref_price,
            )
            return False

        ob = exchange.fetch_order_book(symbol, limit=1)
        asks = ob.get("asks", [])
        bids = ob.get("bids", [])

        if side.upper() == "BUY":
            if not asks:
                logger.warning(
                    "⚠️ [SLIPPAGE_GUARD] '%s' 매도 호가 없음 → 슬리페이지 검증 스킵 (허용)", symbol
                )
                return True
            market_price = Decimal(str(asks[0][0]))
        else:
            if not bids:
                logger.warning(
                    "⚠️ [SLIPPAGE_GUARD] '%s' 매수 호가 없음 → 슬리페이지 검증 스킵 (허용)", symbol
                )
                return True
            market_price = Decimal(str(bids[0][0]))

        # 괴리율 산출 (ref_price > 0 조건으로 0 나눗셈 이미 보장)
        deviation = abs(market_price - ref_price) / ref_price

        if deviation > limit_pct:
            logger.error(
                "🚨 [SLIPPAGE_GUARD] '%s' 슬리페이지 초과 → 주문 REJECTED: "
                "괴리율=%.4f%% > 허용=%.4f%% (기준가=%s, 호가=%s)",
                symbol,
                float(deviation) * 100, float(limit_pct) * 100,
                ref_price, market_price,
            )
            return False

        logger.debug(
            "✅ [SLIPPAGE_GUARD] '%s' 슬리페이지 통과: 괴리율=%.4f%% <= %.4f%%",
            symbol, float(deviation) * 100, float(limit_pct) * 100,
        )
        return True

    except Exception as _slip_exc:
        logger.warning(
            "⚠️ [SLIPPAGE_GUARD] '%s' 슬리페이지 검증 실패 (예외 발생, 허용 처리): %s",
            symbol, _slip_exc,
        )
        return True


# ──────────────────────────────────────────────────────────────────────────
# 🔒 [Phase 6] 동적 Lot Size 헬퍼 — 거래소 Lot Size 기반 수량 quantize 강제
# ──────────────────────────────────────────────────────────────────────────
_LOT_SIZE_CACHE: dict = {}  # {symbol: Decimal(step)} 캐시 (1회 결정 후 영속)



def _get_lot_size_step(
    exchange: ccxt.Exchange,
    symbol: str,
    fallback_step: str = "0.001",
) -> Decimal:
    """
    거래소에서 심볼의 Lot Size 단계(stepSize)를 조회하여 Decimal로 반환합니다.

    바이낸스 BTC/USDT 선물 stepSize = 0.001 BTC.
    조회 실패 시 fallback_step(기본 0.001) 유지 — 예외 발생 안 함.

    Returns:
        Decimal stepSize (예: Decimal("0.001"))
    """
    if symbol in _LOT_SIZE_CACHE:
        return _LOT_SIZE_CACHE[symbol]

    step = Decimal(fallback_step)
    try:
        market = exchange.market(symbol)
        precision_amount = market.get("precision", {}).get("amount")
        if precision_amount is not None:
            # precision['amount']가 소수점 자릿수(int)인 경우: 3 → 0.001
            if isinstance(precision_amount, int):
                step = Decimal(10) ** (-precision_amount)
            else:
                step = Decimal(str(precision_amount))
        else:
            min_amt = market.get("limits", {}).get("amount", {}).get("min")
            if min_amt is not None:
                step = Decimal(str(min_amt))

        if step <= Decimal("0"):
            step = Decimal(fallback_step)

    except Exception as _lot_exc:
        logger.warning(
            "⚠️ [LOT_SIZE] %s stepSize 조회 실패 → fallback=%s 적용: %s",
            symbol, fallback_step, _lot_exc,
        )
        step = Decimal(fallback_step)

    _LOT_SIZE_CACHE[symbol] = step
    logger.info("🔒 [LOT_SIZE] %s stepSize 확정: %s", symbol, step)
    return step


def _quantize_amount(
    amount: Decimal,
    step: Decimal,
    min_order: Decimal = Decimal("0"),
) -> Optional[Decimal]:
    """
    주문 수량을 Lot Size 단계에 맞게 버림(ROUND_DOWN)으로 정화합니다.

    ROUND_DOWN: 올림 시 가용 잔고 초과 → InsufficientFunds 위험 차단.

    Returns:
        정화된 Decimal 또는 None (최소 수량 미달 시)
    """
    try:
        quantized = (amount / step).to_integral_value(rounding=ROUND_DOWN) * step
        if quantized <= Decimal("0") or quantized < min_order:
            return None
        return quantized
    except (InvalidOperation, ZeroDivisionError) as exc:
        logger.error(
            "❌ [QUANTIZE] 수량 정화 실패: amount=%s, step=%s, exc=%s",
            amount, step, exc,
        )
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


def _get_futures_margin_balance(exchange: "ccxt.Exchange") -> tuple[Decimal, Decimal]:
    """
    선물 지갑의 마진 잔고를 정밀 조회하는 헬퍼 함수.
    [선물 지갑 마진 잔고(totalMarginBalance) + 현물 지갑의 BTC 잔고 가치(BTC 수량 × 현재 시장가)]를
    모두 합산한 '통합 총자산'을 계산하여 반환하여 status 및 trading engine 간 일치성을 보장합니다.
    
    Returns:
        tuple[Decimal, Decimal]: (통합 총자산(USDT), 가용 마진 잔고(USDT))
    """
    balance_info = _fetch_balance_with_retry(exchange)
    
    # 기본값 (Fallback)
    total_margin = Decimal(str(balance_info["total"].get("USDT", 0.0)))
    free_margin = Decimal(str(balance_info["free"].get("USDT", 0.0)))
    
    try:
        # 바이낸스 원시 info 객체에서 totalMarginBalance 최우선 참조
        raw_info = balance_info.get("info", {})
        if "totalMarginBalance" in raw_info:
            total_margin = Decimal(str(raw_info["totalMarginBalance"]))
        if "availableBalance" in raw_info:
            free_margin = Decimal(str(raw_info["availableBalance"]))
    except Exception as e:
        logger.warning(f"⚠️ [Margin Balance] 원시 info 객체 파싱 실패, Fallback 적용: {e}")

    # ── [결함 #1 해결] 현물 지갑 내 BTC 잔고 체크 및 실시간 가치 통합 ─────────
    spot_btc = Decimal("0")
    try:
        # CCXT fetch_balance에서 type='spot'으로 현물 잔고 조회
        spot_bal = exchange.fetch_balance({"type": "spot"})
        if spot_bal:
            if "total" in spot_bal and "BTC" in spot_bal["total"]:
                spot_btc = Decimal(str(spot_bal["total"]["BTC"]))
            elif "BTC" in spot_bal:
                if isinstance(spot_bal["BTC"], dict):
                    spot_btc = Decimal(str(spot_bal["BTC"].get("total", 0.0)))
                else:
                    spot_btc = Decimal(str(spot_bal["BTC"]))
    except Exception as spot_exc:
        logger.warning(f"⚠️ [BTC 현물 잔고 조회] 실패 (무시하고 0.0 처리): {spot_exc}")

    if spot_btc > Decimal("0"):
        btc_price = Decimal("0")
        # 1순위: WebSocket 인메모리 큐 최신가 참조 (0ms 지연)
        try:
            if _ohlcv_stream:
                key = _ohlcv_stream._normalize("BTC/USDT")
                q = _ohlcv_stream._queues.get(key)
                if q and len(q) > 0:
                    btc_price = Decimal(str(q[-1][4]))  # index 4 is close price
                    logger.info(f"⚡ [BTC 실시간 시세] WebSocket 인메모리 큐 최신가 참조 성공: {btc_price} USDT")
        except Exception as ws_err:
            logger.warning(f"⚠️ [BTC 실시간 시세] WebSocket 큐 조회 실패: {ws_err}")

        # 2순위 Fallback: ccxt.fetch_ticker REST API
        if btc_price <= Decimal("0"):
            try:
                ticker = exchange.fetch_ticker("BTC/USDT")
                btc_price = Decimal(str(ticker["last"]))
                logger.info(f"📡 [BTC 실시간 시세] ccxt.fetch_ticker REST API 조회 성공: {btc_price} USDT")
            except Exception as rest_err:
                logger.error(f"❌ [BTC 실시간 시세] fetch_ticker REST API 최종 실패: {rest_err}")

        # 실시간 가치 정밀 합산 (Decimal)
        if btc_price > Decimal("0"):
            spot_btc_value = spot_btc * btc_price
            total_margin += spot_btc_value
            logger.info(
                f"💰 [통합 자산 합산 성공] 선물 마진 잔고: {total_margin - spot_btc_value} USDT + "
                f"현물 BTC 잔고 가치: {spot_btc_value} USDT ({spot_btc} BTC × {btc_price} USDT) "
                f"→ 통합 총자산: {total_margin} USDT"
            )
        else:
            logger.warning("⚠️ [통합 자산 합산] BTC 시세를 획득하지 못해 현물 자산 합산을 스킵합니다.")
        
    return total_margin, free_margin


# ─────────────────────────────────────────────
# 🛑 일일 최대손실 서킷브레이커
# ─────────────────────────────────────────────
def _check_daily_loss_breaker(current_equity: Decimal) -> bool:
    """
    당일(KST) 시작 자본 대비 손실이 settings.max_daily_loss_pct에 도달하면
    Redis sys_status=PAUSED로 매매를 동결하고 True를 반환한다.

    - 당일 시작 자본은 그날 첫 호출 시 Redis에 SETNX로 1회 기록(KST 날짜별 키, 2일 만료).
    - 한도(<=0)면 비활성으로 항상 False.
    - 발동 시 텔레그램 긴급 경고를 1회만 발송하고, 이후 매 호출은 상단 pause 가드가 차단.
    - 다음 KST 자정에 새 시작 자본이 기록되어 자동 리셋.
    """
    try:
        limit = Decimal(str(settings.max_daily_loss_pct))
    except (InvalidOperation, TypeError):
        return False
    if limit <= Decimal("0") or _r_sync is None:
        return False
    try:
        kst_date = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        key = f"quantflow:day_start_equity:{kst_date}"
        # 당일 첫 호출이면 시작 자본 기록 (원자적 SETNX, 48h 후 만료)
        if _r_sync.set(key, str(current_equity), nx=True, ex=172800):
            day_start = current_equity
        else:
            day_start = Decimal(_r_sync.get(key) or str(current_equity))
        if day_start <= Decimal("0"):
            return False
        daily_pnl = (current_equity - day_start) / day_start
        if daily_pnl <= -limit:
            already_paused = (_r_sync.get("quantflow:sys_status") == "PAUSED")
            _r_sync.set("quantflow:sys_status", "PAUSED")
            if not already_paused:
                logger.error(
                    "🛑 [서킷브레이커] 일일 손실 한도 도달 → 매매 동결(PAUSED): "
                    "당일 시작 %.2f → 현재 %.2f USDT (%.2f%% ≤ -%.2f%%)",
                    float(day_start), float(current_equity),
                    float(daily_pnl) * 100, float(limit) * 100,
                )
                notifier.send_message(
                    f"🛑 <b>[QuantFlow] 일일 손실 서킷브레이커 발동</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"• <b>당일 손익:</b> <code>{float(daily_pnl)*100:.2f}%</code> "
                    f"(한도 -{float(limit)*100:.1f}%)\n"
                    f"• <b>시작 자본:</b> <code>${float(day_start):,.2f}</code>\n"
                    f"• <b>현재 자본:</b> <code>${float(current_equity):,.2f}</code>\n"
                    f"• <b>조치:</b> <code>매매 자동 동결(PAUSED)</code>\n"
                    f"• 재개하려면 텔레그램에 <code>/resume</code> (당일 자정 자동 리셋)\n"
                    f"━━━━━━━━━━━━━━━━━━━━"
                )
            return True
        return False
    except Exception as exc:
        logger.warning("⚠️ [서킷브레이커] 검사 중 예외 (매매 계속): %s", exc)
        return False


# ─────────────────────────────────────────────
# 🔁 [비동기 DB 헬퍼] 날짜 필터 조회 — async_session 기반
# ─────────────────────────────────────────────
async def _async_query_today_trades(symbol: str, start_of_day_naive: datetime) -> list:
    """
    비동기 세션(core.database.async_session)을 사용하여 오늘 KST 자정 이후
    체결된 TradeHistory를 타임스탬프 오름차순으로 조회합니다.

    Celery 동기 태스크에서는 asyncio.run()으로 감싸 호출합니다.

    Args:
        symbol: 조회할 심볼 (예: "BTC/USDT")
        start_of_day_naive: KST 자정 기준의 timezone-naive datetime

    Returns:
        TradeHistory 객체 리스트 (status == 'FILLED', timestamp 오름차순)
    """
    async with _async_session_factory() as session:
        stmt = (
            sa_select(TradeHistory)
            .where(
                TradeHistory.symbol == symbol,
                TradeHistory.timestamp >= start_of_day_naive,
                TradeHistory.status == "FILLED",
            )
            .order_by(TradeHistory.timestamp.asc())
        )
        result = await session.execute(stmt)
        return result.scalars().all()


async def _async_query_all_filled_sells(symbol: str) -> list:
    """
    비동기 세션(core.database.async_session)을 사용하여 해당 심볼의
    전체 FILLED SELL(숏 진입) 이력을 타임스탬프 오름차순으로 조회합니다.

    generate_daily_report_task()의 Short PnL 계산 로직에서
    asyncio.run()으로 감싸 호출합니다.

    Args:
        symbol: 조회할 심볼 (예: "BTC/USDT")

    Returns:
        TradeHistory 객체 리스트 (side == 'SELL', status == 'FILLED', timestamp 오름차순)
    """
    async with _async_session_factory() as session:
        stmt = (
            sa_select(TradeHistory)
            .where(
                TradeHistory.symbol == symbol,
                TradeHistory.side   == "SELL",
                TradeHistory.status == "FILLED",
            )
            .order_by(TradeHistory.timestamp.asc())
        )
        result = await session.execute(stmt)
        return result.scalars().all()


async def _async_save_trade_history(
    timestamp: datetime,
    symbol: str,
    side: str,
    price: Decimal,
    amount: Decimal,
    status: str,
) -> None:
    """
    TradeHistory 레코드를 비동기 세션(core.database.async_session)으로
    INSERT·COMMIT 합니다.

    ── 왜 이 헬퍼가 필요한가 ──────────────────────────────────────────────
    core/database.py 는 asyncpg 기반 완전 비동기 엔진만 제공합니다.
    Celery 동기 태스크(analyze_and_trade) 내부의 _SyncSession 으로는
    asyncpg 엔진에 커밋할 수 없어 INSERT 전량이 묵살됩니다.

    이 함수는 ORM 객체를 비동기 세션 컨텍스트 내에서 생성·add·commit 하여
    세션 종속성 문제(detached-instance error)를 원천 차단합니다.
    Celery 동기 컨텍스트에서는 asyncio.run()으로 감싸 호출합니다.

    Args:
        timestamp : 체결 시각 (UTC datetime)
        symbol    : 거래 심볼 (예: "BTC/USDT")
        side      : 방향 ("BUY" | "SELL")
        price     : 체결 단가 (Decimal)
        amount    : 체결 수량 (Decimal)
        status    : 주문 상태 ("FILLED" | "PARTIALLY_FILLED" | "REJECTED" | "FAILED" ...)
    """
    async with _async_session_factory() as session:
        record = TradeHistory(
            timestamp=timestamp,
            symbol=symbol,
            side=side,
            price=price,
            amount=amount,
            status=status,
        )
        session.add(record)
        await session.commit()


# ─────────────────────────────────────────────
# 🛡️ [Non-blocking 비동기 격리 래퍼] — asyncio × 동기 세션 교착 원천 차단
# ─────────────────────────────────────────────
def _run_async_safe(coro):
    """
    Celery 동기 워커(prefork)에서 asyncio 코루틴을 스레드 격리로 안전하게 실행합니다.
    매 실행마다 독립된 이벤트 루프 수명 주기를 보장하여 'Event loop is closed' 및
    'attached to a different loop' 에러를 완벽하게 차단합니다.

    Args:
        coro: 실행할 asyncio 코루틴 객체
    Returns:
        코루틴의 반환값
    Raises:
        concurrent.futures.TimeoutError: 15초 이내 완료되지 않은 경우 (무한 블로킹 방지)
    """
    import concurrent.futures

    def _execute_in_isolated_loop(c):
        # 이 함수는 전용 ThreadPoolExecutor 스레드(max_workers=1)에서만 실행되므로
        # 해당 스레드에는 기존 이벤트 루프가 존재하지 않는다.
        # → asyncio.get_event_loop() (Python 3.12+에서 RuntimeError/Deprecation)를
        #   거치지 않고 항상 새 루프를 생성하여 이벤트 루프 불일치를 원천 차단한다.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            return loop.run_until_complete(c)
        finally:
            try:
                # 루프 내 미완료/대기 중인 태스크들 정리
                try:
                    pending = asyncio.all_tasks(loop)
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                loop.close()
            except Exception as close_exc:
                logger.warning(f"⚠️ [비동기 격리] 독립 루프 정리 실패: {close_exc}")
            finally:
                asyncio.set_event_loop(None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(_execute_in_isolated_loop, coro).result(timeout=15)


# ─────────────────────────────────────────────
# 🔄 DB Warm-up: 재시작 시 과거 데이터로 지표 재계산
# ─────────────────────────────────────────────
def _warmup_from_db(symbol: str = "BTC/USDT", lookback: int = 500) -> int:
    """
    봇 재시작 시 DB에 저장된 최근 lookback 개 캔들을 읽어
    compute_all_features()로 지표를 재계산하고, 지표값이 NULL인 레코드를
    **1회 Bulk Upsert**로 일괄 저장합니다.

    ── [최적화 ①] N+1 쿼리 → 1회 Bulk Upsert ───────────────────────────────
    기존: rows 개수(최대 500)만큼 개별 pg_insert 실행 → DB 500번 왕복
    변경: NULL 지표 레코드를 리스트로 모아 execute() 1회로 일괄 처리

    Returns:
        upsert된 레코드 수
    """
    logger.info(
        "🔄 [DB Warm-up] 시작 — symbol=%s, lookback=%d봉", symbol, lookback
    )
    try:
        session = _SyncSession()
        try:
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
        def _normalize_ts(ts: datetime) -> datetime:
            if ts is None:
                return ts
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)

        # DataFrame 재구성 (OHLCV만 — 지표는 재계산)
        df_rows = [
            {
                "timestamp_ms": int(_normalize_ts(r.timestamp).timestamp() * 1000),
                "open":   float(r.open),
                "high":   float(r.high),
                "low":    float(r.low),
                "close":  float(r.close),
                "volume": float(r.volume),
            }
            for r in rows
        ]

        df = pd.DataFrame(df_rows)
        df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

        from worker.indicators import compute_all_features
        df = compute_all_features(df)

        # ── [최적화 ①] NULL 지표 레코드를 리스트로 수집 후 Bulk Upsert ──────────
        # 기존 방식: 레코드마다 개별 session.execute() → N번 DB 왕복
        # 변경 방식: values_list에 모아서 단 1번의 execute()로 일괄 처리
        values_list = []
        for i, r in enumerate(rows):
            # 지표가 NULL인 행만 대상 (정상 행 불필요한 재기록 방지)
            if r.rsi_14 is None or r.bb_upper is None or r.bb_lower is None or getattr(r, 'atr_14', None) is None:
                row_feat = df.iloc[i]
                values_list.append({
                    "timestamp": _normalize_ts(r.timestamp),
                    "symbol":    symbol,
                    "open":      Decimal(str(r.open)),
                    "high":      Decimal(str(r.high)),
                    "low":       Decimal(str(r.low)),
                    "close":     Decimal(str(r.close)),
                    "volume":    Decimal(str(r.volume)),
                    "sma_20":    _to_dec(row_feat.get("sma_20")),
                    "rsi_14":    _to_dec(row_feat.get("rsi_14")),
                    "bb_upper":  _to_dec(row_feat.get("bb_upper")),
                    "bb_lower":  _to_dec(row_feat.get("bb_lower")),
                    "atr_14":    _to_dec(row_feat.get("atr_14")),
                    "macd_line":    _to_dec(row_feat.get("macd_line")),
                    "macd_signal":  _to_dec(row_feat.get("macd_signal")),
                    "macd_hist":    _to_dec(row_feat.get("macd_hist")),
                })

        upsert_count = len(values_list)
        if values_list:
            # 단 1회 execute — DB 왕복을 최대 500회에서 1회로 단축
            bulk_stmt = pg_insert(MarketData).values(values_list).on_conflict_do_update(
                constraint="uq_market_data_ts_symbol",
                set_={
                    "sma_20":      pg_insert(MarketData).excluded.sma_20,
                    "rsi_14":      pg_insert(MarketData).excluded.rsi_14,
                    "bb_upper":    pg_insert(MarketData).excluded.bb_upper,
                    "bb_lower":    pg_insert(MarketData).excluded.bb_lower,
                    "atr_14":      pg_insert(MarketData).excluded.atr_14,
                    "macd_line":   pg_insert(MarketData).excluded.macd_line,
                    "macd_signal": pg_insert(MarketData).excluded.macd_signal,
                    "macd_hist":   pg_insert(MarketData).excluded.macd_hist,
                },
            )
            for session in _get_sync_session():
                session.execute(bulk_stmt)
            logger.info(
                "✅ [DB Warm-up] Bulk Upsert 완료 — 처리 %d봉, 지표 upsert %d건 (DB 왕복 1회)",
                len(rows), upsert_count
            )
        else:
            logger.info(
                "✅ [DB Warm-up] 완료 — 처리 %d봉, NULL 지표 없음 (upsert 불필요)",
                len(rows)
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


# ──────────────────────────────────────────────────────────────────────────
# 📡 시세 데이터 수집 & DB 저장 [Phase 6: WebSocket 큐 우선, ta 완전 제거]
# ──────────────────────────────────────────────────────────────────────────
@celery_app.task(
    bind=True,
    name="worker.tasks.fetch_market_data_task",
    queue="market_data",
    max_retries=3,
    default_retry_delay=10,
)
def fetch_market_data_task(self, symbol: str = "BTC/USDT"):
    """
    [Phase 6 구조]
    1순위: WebSocket 인메모리 큐(deque)에서 OHLCV DataFrame 직접 조회 (0ms 근접)
    2순위: 큐 미성숙 또는 스트림 미활성 시 REST API Fallback (기존 동작 보존)

    [ta 라이브러리 완전 제거]
    - ta.volatility.average_true_range() → indicators.compute_all_features() 통합 ATR
    - ta.trend.macd()                    → indicators.compute_all_features() 통합 MACD
    - 순수 Numpy Wilder 스무딩으로 연산 속도 약 10배 이상 향상
    """
    logger.info(f"📡 OHLCV 수집 및 피처 생성 시작: {symbol}")
    try:
        from worker.indicators import compute_all_features

        # ── 단계 1: WebSocket 인메모리 큐 우선 참조 (0ms 근접) ────────────────
        df = None
        _data_source = "REST-Fallback"
        if _ohlcv_stream is not None and _ohlcv_stream.is_alive(symbol):
            df = _ohlcv_stream.get_latest_df(symbol=symbol, min_candles=60)
            if df is not None:
                _data_source = "WS-Queue"
                logger.debug(
                    "⚡ [fetch_market_data] WebSocket 큐 사용 — %d봉 (레이턴시 0ms)",
                    len(df),
                )

        # ── 단계 2: REST API Fallback (큐 미성숙 시) ──────────────────────────
        if df is None:
            logger.info(
                "📡 [fetch_market_data] REST Fallback 실행 (WS 큐 미성숙) — %s", symbol,
            )
            exchange = get_exchange()
            ohlcv_list = exchange.fetch_ohlcv(symbol=symbol, timeframe="1m", limit=500)

            if not ohlcv_list:
                logger.warning(f"⚠️ 빈 OHLCV 응답: {symbol}")
                return {"status": "empty", "symbol": symbol}

            df = pd.DataFrame(
                ohlcv_list,
                columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
            ).astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
            df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

            # REST Fallback 시 WebSocket 큐도 함께 탁우기 (살아있으면)
            if _ohlcv_stream is not None:
                try:
                    _ohlcv_stream.seed_from_rest(symbol, ohlcv_list)
                except Exception as _seed_exc:
                    logger.warning("⚠️ [OhlcvStream] REST seed 실패 (무시): %s", _seed_exc)

        # ── 피처 엔지니어링 파이프라인 (ta 완전 제거) ─────────────────────────
        # [Phase 6 핵심]
        # 기존: ta.volatility.average_true_range() — Pandas rolling 루프 (GIL 직렬)
        #       ta.trend.macd()                    — indicators.py와 중복 계산
        # 변경: compute_all_features()에 ATR 통합 — 순수 Numpy Wilder 스무딩
        df = compute_all_features(df)

        # [최적화 ③] 계산 결과를 Redis 60초 캐시에 저장
        # → analyze_and_trade의 Regime Classifier가 동일 데이터로 중복 계산하지 않도록
        _set_cached_features(symbol, df)

        last = df.iloc[-1]
        candle_dt = _ensure_utc(int(last["timestamp_ms"]))

        # ── NaN 가드: pd.isna() 기반 일원화 헬퍼 ─────────────────────────────
        def _safe_dec(col: str):
            raw = last.get(col)
            if raw is None:
                return None
            try:
                if pd.isna(raw):
                    return None
            except (TypeError, ValueError):
                pass
            return _to_dec(raw)

        sma_20      = _safe_dec("sma_20")
        rsi_14      = _safe_dec("rsi_14")
        bb_upper    = _safe_dec("bb_upper")
        bb_lower    = _safe_dec("bb_lower")
        atr_14      = _safe_dec("atr_14")      # [Phase 6] indicators.py Numpy ATR
        macd_line   = _safe_dec("macd_line")   # [Phase 6] indicators.py 통합값
        macd_signal = _safe_dec("macd_signal")
        macd_hist   = _safe_dec("macd_hist")

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
                "sma_20":      insert_stmt.excluded.sma_20,
                "rsi_14":      insert_stmt.excluded.rsi_14,
                "bb_upper":    insert_stmt.excluded.bb_upper,
                "bb_lower":    insert_stmt.excluded.bb_lower,
                "atr_14":      insert_stmt.excluded.atr_14,
                "macd_line":   insert_stmt.excluded.macd_line,
                "macd_signal": insert_stmt.excluded.macd_signal,
                "macd_hist":   insert_stmt.excluded.macd_hist,
            },
        )

        for session in _get_sync_session():
            session.execute(stmt)

        logger.info(
            "✅ [fetch_market_data] 완료 — symbol=%s, ts=%s, 소스=%s, ATR=%.4f",
            symbol, candle_dt.isoformat(), _data_source,
            float(atr_14) if atr_14 else float("nan"),
        )
        return {"status": "ok", "symbol": symbol, "timestamp": candle_dt.isoformat()}

    except Exception as exc:
        logger.error(f"❌ OHLCV 수집 실패: {exc}", exc_info=True)
        raise self.retry(exc=exc)



# ──────────────────────────────────────────────────────────────────────────
# [최적화 ③] compute_all_features Redis 60초 캐시 헬퍼
# ──────────────────────────────────────────────────────────────────────────
_FEATURES_CACHE_KEY_PREFIX = "quantflow:features_cache:"
_FEATURES_CACHE_TTL_SEC    = 60  # 60초 TTL — 1분봉 주기에 맞춰 스테일 데이터 방지


def _get_cached_features(symbol: str) -> "pd.DataFrame | None":
    """
    Redis에서 60초 캐시된 compute_all_features() 결과를 조회합니다.

    fetch_market_data_task가 이미 동일 데이터로 피처를 계산했을 때
    analyze_and_trade의 Regime Classifier가 중복 계산하지 않도록 방지합니다.

    Returns:
        캐시 적중 시 DataFrame, 미스 또는 오류 시 None
    """
    if _r_sync is None:
        return None
    try:
        key = f"{_FEATURES_CACHE_KEY_PREFIX}{_normalize_symbol(symbol)}"
        cached_json = _r_sync.get(key)
        if cached_json is None:
            return None
        records = json.loads(cached_json)
        df = pd.DataFrame(records)
        logger.debug("⚡ [Features 캐시] Redis 캐시 HIT — symbol=%s, rows=%d", symbol, len(df))
        return df
    except Exception as _cache_exc:
        logger.warning("⚠️ [Features 캐시] 조회 실패 (재계산 진행): %s", _cache_exc)
        return None


def _set_cached_features(symbol: str, df: "pd.DataFrame") -> None:
    """
    compute_all_features() 결과를 Redis에 60초 TTL로 캐시합니다.
    직렬화 실패 시 조용히 무시하여 캐시 계층이 핵심 로직을 방해하지 않습니다.
    """
    if _r_sync is None:
        return
    try:
        key = f"{_FEATURES_CACHE_KEY_PREFIX}{_normalize_symbol(symbol)}"
        # float 변환으로 JSON 직렬화 가능하게 처리 (NaN → null)
        records = df.where(pd.notnull(df), None).to_dict(orient="records")
        _r_sync.setex(key, _FEATURES_CACHE_TTL_SEC, json.dumps(records))
        logger.debug("💾 [Features 캐시] Redis 캐시 SET — symbol=%s, rows=%d, TTL=%ds",
                     symbol, len(df), _FEATURES_CACHE_TTL_SEC)
    except Exception as _cache_exc:
        logger.warning("⚠️ [Features 캐시] 저장 실패 (무시): %s", _cache_exc)


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
def analyze_and_trade(self, symbol: str = "BTC/USDT"):  # noqa: C901
    logger.info(f"🧠 QuantFlow 하이브리드 의사결정 엔진 가동: {symbol}")

    # ── [결함 #1 수정] 심볼별 Redis 분산 락 — 중복 트리거 즉시 차단 ──────────
    # [최적화 ②] 모듈 레벨 싱글턴 _r_sync 공유 (반복 연결 생성 제거)
    _lock_key = f"{_ORDER_DEDUP_LOCK_PREFIX}{_normalize_symbol(symbol)}"
    _r_lock = _r_sync  # 싱글턴 재사용
    _lock_held = False  # 우리가 락을 실제로 점유했는지 추적 — finally에서 해제 판단용
    try:
        if _r_lock is None:
            # 싱글턴 초기화 실패 시에만 임시 연결 생성
            _r_lock = _sync_redis_lib.Redis.from_url(
                settings.redis_url, decode_responses=True, socket_connect_timeout=2
            )
        # NX=True: 키가 없을 때만 SET (원자적 SETNX)
        # EX: TTL(초) 자동 설정 → 워커 크래시 시 락 영구 잠금 방지
        _lock_acquired = _r_lock.set(_lock_key, "1", nx=True, ex=_ORDER_DEDUP_LOCK_TTL_SEC)
        if _lock_acquired:
            _lock_held = True
        if not _lock_acquired:
            logger.warning(
                "🔒 [중복 주문 방지] '%s' 심볼 락 점유 중 — 이번 트리거 Skip (TTL=%ds)",
                symbol, _ORDER_DEDUP_LOCK_TTL_SEC,
            )
            return {"status": "duplicate_trigger_skipped", "symbol": symbol}
        logger.info(
            "🔑 [중복 주문 방지] '%s' 심볼 락 획득 성공 (TTL=%ds) — 매매 로직 진입",
            symbol, _ORDER_DEDUP_LOCK_TTL_SEC,
        )
    except Exception as _lock_exc:
        # Redis 연결 실패 시 락 없이 진행 (가용성 우선) — 단, 경고 로그 기록
        logger.warning(
            "⚠️ [중복 주문 방지] Redis 락 획득 실패 (락 없이 진행): %s", _lock_exc
        )

    try:
        now_utc = datetime.now(timezone.utc)

        # ── [원격 제어] /pause 일시정지 가드 ──────────────────────────────────
        # handle_telegram_command()이 quantflow:sys_status="PAUSED"를 기록하면
        # 신규 분석/진입/청산을 전면 동결한다. (이전엔 키를 기록만 하고 읽지 않아
        # /pause가 무력화되어 있었음 — 실거래 안전장치 복원)
        # Redis 조회 실패 시에는 가용성 우선으로 매매를 계속한다(기존 동작 유지).
        try:
            _status_r = _r_sync if _r_sync is not None else _sync_redis_lib.Redis.from_url(
                settings.redis_url, decode_responses=True, socket_connect_timeout=2
            )
            _sys_status = _status_r.get("quantflow:sys_status")
            if _sys_status is not None and str(_sys_status).upper() == "PAUSED":
                logger.warning("⏸️ [원격 제어] 시스템 PAUSED 상태 — analyze_and_trade 동결 (이번 트리거 스킵)")
                return {"status": "system_paused", "symbol": symbol}
        except Exception as _status_exc:
            logger.warning("⚠️ [원격 제어] sys_status 조회 실패 (매매 계속 진행): %s", _status_exc)

        exchange = get_exchange()

        # [이슈 2 해결] 시간 균열(NTP Drift)로 인한 타임스탬프 거절 방어 이중 Failsafe
        exchange.options['adjustForTimeDifference'] = True
        exchange.options['recvWindow'] = 10000

        # 1. 실시간 자산 잔고 트래킹 — 3회 자동 재시도 보장 (8차 확장)
        try:
            usdt_balance, usdt_free = _get_futures_margin_balance(exchange)
        except Exception as e:
            logger.error("❌ 실시간 지갑 잔고 조회 최종 실패 (%d회 재시도 소진): %s", _BALANCE_RETRY_MAX, e)
            return {"status": "balance_fetch_failed"}

        # ── [서킷브레이커] 일일 최대손실 도달 시 매매 동결 ──────────────────────
        # 당일(KST) 시작 자본 대비 손실이 한도 초과면 sys_status=PAUSED로 전환하고 중단.
        # (한도 미설정 시 비활성 — 기존 동작 영향 없음)
        if _check_daily_loss_breaker(usdt_balance):
            return {"status": "daily_loss_breaker_tripped", "symbol": symbol}

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
                            _r.timestamp.astimezone(timezone.utc).timestamp() * 1000
                            if _r.timestamp.tzinfo else
                            _r.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000
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

            # ─────────────────────────────────────────────────────────────────
            # 3. 🛡️ [수석 아키텍트 명세 #2] 포지션 판정 파이프라인 — 100% 원격 API 기반
            # ─────────────────────────────────────────────────────────────────
            # 구형 로직(로컬 TradeHistory DB 조회 → last_trade.side 비교) 완전 소각.
            # 바이낸스 선물 원격 API에서 실제 positionAmt를 직접 조회·파싱하여 판정.
            #
            # [단방향(One-Way) 포지션 파싱 규칙 — 수석 아키텍트 명세 #2]
            #   positionAmt > 0  →  current_position = "LONG"
            #   positionAmt < 0  →  current_position = "SHORT"
            #   positionAmt == 0 →  current_position = "FLAT"
            #
            # [entryPrice 연동 — 수석 아키텍트 명세 #3]
            #   entry_price = Decimal(str(pos.get('entryPrice', 0)))
            #   API 응답에서 직접 추출하여 로컬 DB 참조를 완전히 대체.
            #
            # [pos_contracts — 청산 수량 결정]
            #   abs(positionAmt) 값으로 환매수/청산 주문 수량을 결정.
            #   DB last_trade.amount 참조를 완전히 대체.
            # ─────────────────────────────────────────────────────────────────
            current_position: str = "FLAT"
            entry_price: Decimal | None = None
            pos_contracts: Decimal = Decimal("0")  # 포지션 절대 수량 (청산 시 사용)
            _api_pos_raw: dict = {}                # API info 원본 응답 (updateTime 추출용)

            try:
                # ── [1순위] CCXT fetch_positions 추상화 레이어 시도 ──────────
                # exchange.fetch_positions([symbol]) — Binance USDM Futures 기준
                # 내부적으로 fapiPrivateGetPositionRisk 를 호출하여 통합 응답 반환.
                if exchange.has.get("fetchPositions"):
                    _raw_positions = exchange.fetch_positions([symbol])
                    for _p in _raw_positions:
                        # positionAmt는 CCXT 표준화 필드가 아닌 바이낸스 raw 필드이므로
                        # info 딕셔너리에서 직접 추출하여 float 안전 변환 수행.
                        _info = _p.get("info", {})
                        _pa_raw = _info.get("positionAmt", "0")
                        try:
                            _pa_float = float(_pa_raw)
                        except (TypeError, ValueError):
                            _pa_float = 0.0

                        # 단방향(One-Way) 포지션 판정: positionAmt 부호로 방향 결정
                        if _pa_float > 0.0:
                            current_position = "LONG"
                            entry_price      = Decimal(str(_p.get("entryPrice", 0) or 0))
                            pos_contracts    = Decimal(str(abs(_pa_float)))
                            _api_pos_raw     = _info
                            break
                        elif _pa_float < 0.0:
                            current_position = "SHORT"
                            entry_price      = Decimal(str(_p.get("entryPrice", 0) or 0))
                            pos_contracts    = Decimal(str(abs(_pa_float)))
                            _api_pos_raw     = _info
                            break
                        # _pa_float == 0.0 → 이 심볼 포지션 없음(FLAT), 루프 계속

                else:
                    # ── [2순위 Fallback] fapiPrivateGetPositionRisk 직접 호출 ──
                    # fetchPositions 미지원 환경 또는 CCXT 버전 호환 이슈 시 사용.
                    _symbol_no_slash = _normalize_symbol(symbol)  # "BTC/USDT" → "BTCUSDT"
                    _pos_risk_list = exchange.fapiPrivateGetPositionRisk(
                        params={"symbol": _symbol_no_slash}
                    )
                    if isinstance(_pos_risk_list, list):
                        for _pr in _pos_risk_list:
                            _pa_raw = _pr.get("positionAmt", "0")
                            try:
                                _pa_float = float(_pa_raw)
                            except (TypeError, ValueError):
                                _pa_float = 0.0

                            if _pa_float > 0.0:
                                current_position = "LONG"
                                entry_price      = Decimal(str(_pr.get("entryPrice", 0) or 0))
                                pos_contracts    = Decimal(str(abs(_pa_float)))
                                _api_pos_raw     = _pr
                                break
                            elif _pa_float < 0.0:
                                current_position = "SHORT"
                                entry_price      = Decimal(str(_pr.get("entryPrice", 0) or 0))
                                pos_contracts    = Decimal(str(abs(_pa_float)))
                                _api_pos_raw     = _pr
                                break

                logger.info(
                    "📡 [포지션 판정 - API 완료] symbol=%s → position=%s, "
                    "entryPrice=%s, posAmt=%s, positionAmt_raw=%s",
                    symbol, current_position,
                    str(entry_price), str(pos_contracts),
                    _api_pos_raw.get("positionAmt", "N/A"),
                )

            except Exception as _pos_api_exc:
                # API 조회 실패 시 안전하게 FLAT 처리 → 신규 진입/추가 주문 봉쇄.
                # (포지션 불명 상태에서 추가 진입은 자산 위험을 배가시킬 수 있음)
                logger.error(
                    "🚨 [포지션 판정 - API 실패] 원격 포지션 조회 실패 → FLAT 보수 처리 (신규 주문 봉쇄): %s",
                    _pos_api_exc, exc_info=True,
                )
                current_position = "FLAT"
                entry_price      = None
                pos_contracts    = Decimal("0")

            # 4. 🛡️ [손절 방패 (우선순위 1위)] 리스크 관리 작동 검사
            if current_position == "SHORT" and entry_price:
                # 숏 포지션 수익률: 가격이 하락해야 수익 (+)
                price_return = (entry_price - current_close) / entry_price
                if price_return <= STOP_LOSS_THRESHOLD:
                    logger.warning(f"🚨 [손절 방패 가동] 평단가: {entry_price} -> 현재가: {current_close} ({price_return*100:.2f}%)")
                    
                    # [동적 수량 계산 - 손절 청산]
                    # API에서 받은 실제 포지션 절대 수량(pos_contracts)으로 환매수(BUY).
                    # 구형 DB last_trade.amount 참조를 완전히 대체.
                    calculated_amount = pos_contracts
                    
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

                    # 이력 저장 — [교착 방지] _run_async_safe()로 스레드 격리 실행
                    # asyncio.run() 직접 호출 시 동기 세션 홀드 중 커넥션 풀 경합 Deadlock 유발.
                    sl_record_amount = sl_result["filled_amount"] if sl_result["filled_amount"] > Decimal("0") else calculated_amount
                    _run_async_safe(_async_save_trade_history(
                        timestamp=datetime.now(timezone.utc),
                        symbol=symbol,
                        side="BUY",
                        price=sl_result["filled_price"],
                        amount=sl_record_amount,
                        status=sl_result["status"],
                    ))
                    logger.info(
                        "🗄️ [STOP_LOSS_SHIELD] DB 이력 저장 완료 (스레드 격리 비동기): status=%s, order_id=%s",
                        sl_result["status"], sl_result["order_id"]
                    )

                    return {"status": f"stop_loss_{sl_result['status'].lower()}", "order_id": sl_result["order_id"]}

            elif current_position == "LONG" and entry_price:
                # 롱 포지션 수익률: 가격이 상승해야 수익 (+)
                price_return = (current_close - entry_price) / entry_price
                if price_return <= STOP_LOSS_THRESHOLD:
                    logger.warning(f"🚨 [손절 방패 가동] 평단가: {entry_price} -> 현재가: {current_close} ({price_return*100:.2f}%)")
                    
                    calculated_amount = pos_contracts
                    
                    # 방어적 검증 (Short-circuit): 최소 주문 수량 미만 검사
                    if calculated_amount <= Decimal("0") or calculated_amount < MIN_ORDER_BTC:
                        logger.warning(
                            f"⏸️  [손절 방패] 계산된 청산 수량이 부족하여 주문 생략: "
                            f"계산된 수량={calculated_amount}, 최소 필요={MIN_ORDER_BTC}"
                        )
                        return {"status": "insufficient_calculated_amount"}

                    # 🚀 프로덕션 등급 주문 집행 파이프라인 호출 (손절 방패)
                    sl_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="SELL",  # 롱 포지션 청산은 SELL
                        amount=calculated_amount,
                        trigger_type="STOP_LOSS_SHIELD",
                        fallback_price=current_close,
                        confidence=1.0,
                        usdt_balance=usdt_balance,
                    )

                    # 이력 저장
                    sl_record_amount = sl_result["filled_amount"] if sl_result["filled_amount"] > Decimal("0") else calculated_amount
                    _run_async_safe(_async_save_trade_history(
                        timestamp=datetime.now(timezone.utc),
                        symbol=symbol,
                        side="SELL",
                        price=sl_result["filled_price"],
                        amount=sl_record_amount,
                        status=sl_result["status"],
                    ))
                    logger.info(
                        "🗄️ [STOP_LOSS_SHIELD] DB 이력 저장 완료 (스레드 격리 비동기): status=%s, order_id=%s",
                        sl_result["status"], sl_result["order_id"]
                    )

                    return {"status": f"stop_loss_{sl_result['status'].lower()}", "order_id": sl_result["order_id"]}

            # 4-1. ⏱️ [v9.2] 이중 리스크 가드 — 타임아웃 가드 + 트레일링 스탑 가드 (우선순위 2위)
            # ═══════════════════════════════════════════════════════════════════════
            # [Failsafe 순서]
            #   Guard-A (최우선): 타임아웃 EXIT — 4시간 횡보 시 자금 회전을 위한 시장가 청산
            #   Guard-B        : 트레일링 스탑 — Peak ROI +15% 터치 후 5% 반납 시 익절 잠금
            #   Guard-C        : 하드 TP/SL EXIT — 기존 고정 임계치 강제 청산 (하위 호환 유지)
            # ───────────────────────────────────────────────────────────────────────
            if current_position == "SHORT" and entry_price:
                # 숏 포지션 수익률 (진입가 - 현재가) / 진입가
                price_return = (entry_price - current_close) / entry_price
                now_utc_check = datetime.now(timezone.utc)

                # 진입 시각: API updateTime 필드에서 추출하여 보유 기간 산출.
                # _api_pos_raw['updateTime'] — 바이낸스 포지션 마지막 업데이트 밀리초 타임스탬프.
                # 구형 DB last_trade.timestamp 참조를 완전히 대체.
                entry_ts: datetime | None = None
                try:
                    _update_time_ms = int(_api_pos_raw.get("updateTime", 0) or 0)
                    if _update_time_ms > 0:
                        entry_ts = datetime.fromtimestamp(
                            _update_time_ms / 1000.0, tz=timezone.utc
                        )
                except Exception as _ts_exc:
                    logger.warning("⚠️ [포지션 진입 시각] updateTime 파싱 실패: %s", _ts_exc)

                if entry_ts is not None:
                    minutes_held = (now_utc_check - entry_ts).total_seconds() / 60.0
                else:
                    # updateTime 파싱 실패 시 타임아웃 가드를 비활성화 (0분 처리)
                    minutes_held = 0.0
                    logger.warning(
                        "⚠️ [포지션 보유 시간] updateTime 없음 → 타임아웃 가드 비활성화 (이번 턴 스킵)"
                    )

                # ── [Guard-B] Peak ROI 추적 레지스터 업데이트 ─────────────────────
                # 매 루프마다 현재 수익률이 과거 최고점을 경신하면 갱신.
                # price_return이 음수(손실 중)이면 기존 peak 값은 보존됨.
                # 🔑 심볼 정규화: 바이낸스 raw "BTCUSDT" ↔ 내부 "BTC/USDT" KeyMismatch 방지
                _reg_key = _normalize_symbol(symbol)
                _current_peak = _peak_roi_register.get(_reg_key, Decimal("0"))
                if price_return > _current_peak:
                    _peak_roi_register[_reg_key] = price_return
                    logger.info(
                        "📈 [TRAILING_STOP] Peak ROI 신고점 갱신: symbol=%s (key=%s), peak=%.2f%%, current=%.2f%%",
                        symbol, _reg_key, float(price_return) * 100, float(price_return) * 100,
                    )
                _current_peak = _peak_roi_register.get(_reg_key, Decimal("0"))

                _hard_trigger: str | None = None
                _hard_reason: str = ""

                # ── [Guard-A] 타임아웃 EXIT (최우선) ─────────────────────────────
                # 4시간(240분) 이상 횡보 시 지표 신호 불문 즉시 시장가 청산.
                # entry_ts가 유효한 경우에만 발동 (updateTime 파싱 실패 시 비활성화).
                if entry_ts is not None and minutes_held >= MAX_POSITION_MINUTES:
                    _hard_trigger = "TIMEOUT_EXIT"
                    _hard_reason  = (
                        f"장기 횡보 타임아웃 — "
                        f"보유 {minutes_held:.0f}분 → 상한 {MAX_POSITION_MINUTES}분 초과"
                    )

                # ── [Guard-B] 트레일링 스탑 (타임아웃 미발동 시 검사) ────────────
                # Peak ROI가 TRAILING_STOP_ACTIVATION_ROI(+15%) 이상 달성된 적 있으며,
                # 현재 수익률이 고점 대비 TRAILING_STOP_DRAWDOWN(5%) 이상 반납된 순간 발동.
                elif (
                    _current_peak >= TRAILING_STOP_ACTIVATION_ROI
                    and (_current_peak - price_return) >= TRAILING_STOP_DRAWDOWN
                ):
                    _hard_trigger = "TRAILING_STOP_EXIT"
                    _hard_reason  = (
                        f"익절 보존 가드 발동 — "
                        f"Peak ROI {float(_current_peak)*100:+.2f}% 달성 후 "
                        f"현재 {float(price_return)*100:+.2f}%로 "
                        f"{float(_current_peak - price_return)*100:.2f}% 반납"
                    )

                # ── [Guard-C] 하드 TP / 하드 SL (기존 로직 하위 호환 유지) ───────
                elif price_return >= HARD_TP_THRESHOLD:
                    _hard_trigger = "HARD_TP_EXIT"
                    _hard_reason  = f"숏 수익률 {price_return*100:+.2f}% ≥ +{float(HARD_TP_THRESHOLD)*100:.1f}% 하드 익절"
                elif price_return <= HARD_SL_THRESHOLD:
                    _hard_trigger = "HARD_SL_EXIT"
                    _hard_reason  = f"숏 수익률 {price_return*100:+.2f}% ≤ {float(HARD_SL_THRESHOLD)*100:.1f}% 하드 손절"

                if _hard_trigger:
                    logger.warning(
                        "🔔 [%s] 숏 강제 청산 발동: %s (평단=$%s, 현재=$%s, 보유=%.0f분)",
                        _hard_trigger, _hard_reason, entry_price, current_close, minutes_held,
                    )
                    # 청산 수량: API에서 받은 포지션 절대 수량(pos_contracts) 사용.
                    # 구형 DB last_trade.amount 참조를 완전히 대체.
                    _hard_amount = pos_contracts
                    if _hard_amount <= Decimal("0") or _hard_amount < MIN_ORDER_BTC:
                        logger.warning("[%s] 숏 청산 수량 부족 → 청산 스킵: %s", _hard_trigger, _hard_amount)
                        return {"status": "insufficient_amount_for_hard_exit"}

                    _hard_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="BUY",  # 숏 청산 (환매수)
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
                    # 이력 저장 — [교착 방지] _run_async_safe()로 스레드 격리 실행
                    _run_async_safe(_async_save_trade_history(
                        timestamp=datetime.now(timezone.utc),
                        symbol=symbol,
                        side="BUY",
                        price=_hard_result["filled_price"],
                        amount=_hard_rec_amount,
                        status=_hard_result["status"],
                    ))

                    # ── 청산 완료 후 Peak ROI 레지스터 초기화 (다음 포지션에서 재출발) ──
                    # 🔑 정규화된 키로 삭제하여 잔류 데이터 방지
                    _peak_roi_register.pop(_normalize_symbol(symbol), None)
                    logger.info(
                        "🧹 [%s] Peak ROI 레지스터 초기화 완료 (symbol=%s, key=%s)",
                        _hard_trigger, symbol, _normalize_symbol(symbol),
                    )

                    # ── 가드별 차별화 텔레그램 알림 발송 ─────────────────────────
                    if _hard_trigger == "TIMEOUT_EXIT":
                        notifier.send_message(
                            f"⏱️ <b>[TIMEOUT_EXIT] 장기 횡보로 인한 타임아웃 청산 완료</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                            f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                            f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                            f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                            f"• <b>수익률:</b> <code>{float(price_return)*100:+.2f}%</code>\n"
                            f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━"
                        )
                    elif _hard_trigger == "TRAILING_STOP_EXIT":
                        notifier.send_message(
                            f"📈 <b>[TRAILING_STOP_EXIT] 익절 보존 가드 발동! 수익 잠금 완료</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                            f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                            f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                            f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                            f"• <b>최고 수익률(Peak):</b> <code>{float(_current_peak)*100:+.2f}%</code>\n"
                            f"• <b>청산 시 수익률:</b> <code>{float(price_return)*100:+.2f}%</code>\n"
                            f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━"
                        )
                    else:
                        # Guard-C: 하드 TP/SL 기존 포맷 유지
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

            elif current_position == "LONG" and entry_price:
                # 롱 포지션 수익률 (현재가 - 진입가) / 진입가
                price_return = (current_close - entry_price) / entry_price
                now_utc_check = datetime.now(timezone.utc)

                entry_ts: datetime | None = None
                try:
                    _update_time_ms = int(_api_pos_raw.get("updateTime", 0) or 0)
                    if _update_time_ms > 0:
                        entry_ts = datetime.fromtimestamp(
                            _update_time_ms / 1000.0, tz=timezone.utc
                        )
                except Exception as _ts_exc:
                    logger.warning("⚠️ [포지션 진입 시각] updateTime 파싱 실패: %s", _ts_exc)

                if entry_ts is not None:
                    minutes_held = (now_utc_check - entry_ts).total_seconds() / 60.0
                else:
                    minutes_held = 0.0
                    logger.warning(
                        "⚠️ [포지션 보유 시간] updateTime 없음 → 타임아웃 가드 비활성화 (이번 턴 스킵)"
                    )

                # ── [Guard-B] Peak ROI 추적 레지스터 업데이트 ─────────────────────
                _reg_key = _normalize_symbol(symbol)
                _current_peak = _peak_roi_register.get(_reg_key, Decimal("0"))
                if price_return > _current_peak:
                    _peak_roi_register[_reg_key] = price_return
                    logger.info(
                        "📈 [TRAILING_STOP] Peak ROI 신고점 갱신: symbol=%s (key=%s), peak=%.2f%%, current=%.2f%%",
                        symbol, _reg_key, float(price_return) * 100, float(price_return) * 100,
                    )
                _current_peak = _peak_roi_register.get(_reg_key, Decimal("0"))

                _hard_trigger: str | None = None
                _hard_reason: str = ""

                # ── [Guard-A] 타임아웃 EXIT (최우선) ─────────────────────────────
                if entry_ts is not None and minutes_held >= MAX_POSITION_MINUTES:
                    _hard_trigger = "TIMEOUT_EXIT"
                    _hard_reason  = (
                        f"장기 횡보 타임아웃 — "
                        f"보유 {minutes_held:.0f}분 → 상한 {MAX_POSITION_MINUTES}분 초과"
                    )

                # ── [Guard-B] 트레일링 스탑 (타임아웃 미발동 시 검사) ────────────
                elif (
                    _current_peak >= TRAILING_STOP_ACTIVATION_ROI
                    and (_current_peak - price_return) >= TRAILING_STOP_DRAWDOWN
                ):
                    _hard_trigger = "TRAILING_STOP_EXIT"
                    _hard_reason  = (
                        f"익절 보존 가드 발동 — "
                        f"Peak ROI {float(_current_peak)*100:+.2f}% 달성 후 "
                        f"현재 {float(price_return)*100:+.2f}%로 "
                        f"{float(_current_peak - price_return)*100:.2f}% 반납"
                    )

                # ── [Guard-C] 하드 TP / 하드 SL ───────
                elif price_return >= HARD_TP_THRESHOLD:
                    _hard_trigger = "HARD_TP_EXIT"
                    _hard_reason  = f"롱 수익률 {price_return*100:+.2f}% ≥ +{float(HARD_TP_THRESHOLD)*100:.1f}% 하드 익절"
                elif price_return <= HARD_SL_THRESHOLD:
                    _hard_trigger = "HARD_SL_EXIT"
                    _hard_reason  = f"롱 수익률 {price_return*100:+.2f}% ≤ {float(HARD_SL_THRESHOLD)*100:.1f}% 하드 손절"

                if _hard_trigger:
                    logger.warning(
                        "🔔 [%s] 롱 강제 청산 발동: %s (평단=$%s, 현재=$%s, 보유=%.0f분)",
                        _hard_trigger, _hard_reason, entry_price, current_close, minutes_held,
                    )
                    _hard_amount = pos_contracts
                    if _hard_amount <= Decimal("0") or _hard_amount < MIN_ORDER_BTC:
                        logger.warning("[%s] 롱 청산 수량 부족 → 청산 스킵: %s", _hard_trigger, _hard_amount)
                        return {"status": "insufficient_amount_for_hard_exit"}

                    _hard_result: OrderResult = _execute_order_pipeline(
                        exchange=exchange,
                        symbol=symbol,
                        side="SELL",  # 롱 청산은 SELL
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
                    # 이력 저장 — [교착 방지] _run_async_safe()로 스레드 격리 실행
                    _run_async_safe(_async_save_trade_history(
                        timestamp=datetime.now(timezone.utc),
                        symbol=symbol,
                        side="SELL",
                        price=_hard_result["filled_price"],
                        amount=_hard_rec_amount,
                        status=_hard_result["status"],
                    ))

                    # ── 청산 완료 후 Peak ROI 레지스터 초기화 ──
                    _peak_roi_register.pop(_normalize_symbol(symbol), None)
                    logger.info(
                        "🧹 [%s] Peak ROI 레지스터 초기화 완료 (symbol=%s, key=%s)",
                        _hard_trigger, symbol, _normalize_symbol(symbol),
                    )

                    # ── 가드별 차별화 텔레그램 알림 발송 ─────────────────────────
                    if _hard_trigger == "TIMEOUT_EXIT":
                        notifier.send_message(
                            f"⏱️ <b>[TIMEOUT_EXIT] 장기 횡보로 인한 타임아웃 청산 완료</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                            f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                            f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                            f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                            f"• <b>수익률:</b> <code>{float(price_return)*100:+.2f}%</code>\n"
                            f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━"
                        )
                    elif _hard_trigger == "TRAILING_STOP_EXIT":
                        notifier.send_message(
                            f"📈 <b>[TRAILING_STOP_EXIT] 익절 보존 가드 발동! 수익 잠금 완료</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                            f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                            f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                            f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                            f"• <b>최고 수익률(Peak):</b> <code>{float(_current_peak)*100:+.2f}%</code>\n"
                            f"• <b>청산 시 수익률:</b> <code>{float(price_return)*100:+.2f}%</code>\n"
                            f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━"
                        )
                    else:
                        # Guard-C: 하드 TP/SL 기존 포맷 유지
                        notifier.send_message(
                            f"🔔 <b>[QuantFlow] {_hard_trigger} (Long Exit)</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"• <b>사유:</b> <code>{_hard_reason}</code>\n"
                            f"• <b>평단가:</b> <code>${float(entry_price):,.2f}</code>\n"
                            f"• <b>청산가:</b> <code>${float(_hard_result['filled_price']):,.2f}</code>\n"
                            f"• <b>수량:</b> <code>{float(_hard_rec_amount):.4f} BTC</code>\n"
                            f"• <b>상태:</b> <code>{_hard_result['status']}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━"
                        )
                    return {"status": f"{_hard_trigger.lower()}_{_hard_result['status'].lower()}", "order_id": _hard_result["order_id"]}

            # 5. 🎯 Predictor 매매 판단 (동적 타입 가드 적용 및 리턴 규격 불일치 해소)
            action_result = _predictor.predict(latest)
            
            if isinstance(action_result, dict):
                action = str(action_result.get("action", action_result.get("status", "HOLD"))).upper()
                confidence = float(action_result.get("confidence", 1.0))
            else:
                action = str(action_result).upper()
                confidence = 1.0
            
            # [이슈 1 해결] 추출된 시그널이 HOLD 거나 HOLD_OR_LOW_CONFIDENCE 인 경우 모두 스킵
            if action in ("HOLD", "HOLD_OR_LOW_CONFIDENCE"):
                return {"status": "hold_or_low_confidence", "confidence": confidence}

            # 6. 📈 [v5.0] 피라미딩(Pyramiding) 매매 로직 — 시장 국면 동적 제어 엔진
            # ══════════════════════════════════════════════════════════════════════
            # [Regime Classifier 연동]
            #   추세 국면(TREND): ADX>=25 AND ATR>=240봉 평균ATR×1.5 → 공격성 유지
            #   횡보 국면(CHOP) : 위 조건 불충족 → 피라미딩 봉쇄, 버퍼 0.5% 상향
            #   FLAT_HOLD 모드 : 지표 NaN → 신규 진입/불타기 전면 차단
            # ══════════════════════════════════════════════════════════════════════

            # ── [A] 시장 국면 판독기(Regime Classifier) 실행 ──────────────────────
            # [최적화 ③] Redis 캐시 우선 조회 → fetch_market_data_task가 이미 계산한 결과 재사용
            _regime_df: pd.DataFrame | None = _get_cached_features(symbol)
            if _regime_df is not None:
                logger.debug("⚡ [REGIME] Redis 캐시 HIT — compute_all_features 중복 계산 생략")
            else:
                # 캐시 미스: WebSocket 큐 → DB 순서로 폴백
                try:
                    if _ohlcv_stream is not None and _ohlcv_stream.is_alive(symbol):
                        _regime_df = _ohlcv_stream.get_latest_df(symbol=symbol, min_candles=60)
                        if _regime_df is not None:
                            from worker.indicators import compute_all_features as _caf_regime
                            _regime_df = _caf_regime(_regime_df)
                            _set_cached_features(symbol, _regime_df)  # 다음 호출 대비 캐시
                except Exception as _regime_df_exc:
                    logger.warning("⚠️ [REGIME] WebSocket 큐 DataFrame 조회 실패 → DB 폴백: %s", _regime_df_exc)
                    _regime_df = None

                # WebSocket 큐 미성숙 또는 실패 시 DB 기록으로 폴백
                if _regime_df is None:
                    try:
                        _fallback_rows = (
                            session.query(MarketData)
                            .filter(MarketData.symbol == symbol)
                            .order_by(desc(MarketData.timestamp))
                            .limit(500)
                            .all()
                        )
                        if len(_fallback_rows) >= 30:
                            _fallback_rows.reverse()  # oldest → newest
                            _df_fallback = pd.DataFrame([
                                {
                                    "timestamp_ms": int(
                                        _r.timestamp.astimezone(timezone.utc).timestamp() * 1000
                                        if _r.timestamp.tzinfo else
                                        _r.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000
                                    ),
                                    "open":   float(_r.open),
                                    "high":   float(_r.high),
                                    "low":    float(_r.low),
                                    "close":  float(_r.close),
                                    "volume": float(_r.volume),
                                }
                                for _r in _fallback_rows
                            ])
                            _df_fallback = _df_fallback.sort_values(
                                "timestamp_ms", ascending=True
                            ).reset_index(drop=True)
                            from worker.indicators import compute_all_features as _caf_fallback
                            _regime_df = _caf_fallback(_df_fallback)
                            _set_cached_features(symbol, _regime_df)  # 다음 호출 대비 캐시
                    except Exception as _fb_exc:
                        logger.warning("⚠️ [REGIME] DB 폴백 DataFrame 구성 실패: %s", _fb_exc)
                        _regime_df = None

            # 국면 판독 실행
            _regime: str = "FLAT_HOLD"
            _regime_meta: dict = {"reason": "DataFrame 미구성 → FLAT_HOLD 보수 처리"}
            if _regime_df is not None and len(_regime_df) > 0:
                _regime, _regime_meta = _classify_market_regime(
                    df=_regime_df,
                    adx_threshold=ADX_TREND_THRESHOLD,
                    atr_ratio=ATR_EXPANSION_RATIO,
                    atr_lookback=ATR_LOOKBACK_CANDLES,
                )
            else:
                logger.warning("⚠️ [REGIME] DataFrame 미구성 → FLAT_HOLD 보수 처리 (신규 진입 차단)")

            # ── [FLAT_HOLD 모드 분기] 지표 NaN → 신규 진입/불타기 전면 차단 ──────────
            if _regime == "FLAT_HOLD":
                logger.warning(
                    "🛑 [FLAT_HOLD] 최고 보수 모드 발동 — 신규 진입/불타기 전면 차단: %s",
                    _regime_meta.get("reason", ""),
                )
                return {
                    "status": "flat_hold_mode",
                    "reason": _regime_meta.get("reason", "NaN Failsafe"),
                    "symbol": symbol,
                }

            # ── [B] 피라미딩 가드 임계치 동적 바인딩 ─────────────────────────────────
            # 국면 판정 결과에 따라 PRICE_BUFFER_PCT 및 PYRAMID_MAX_ADDS를 동적으로 가변 바인딩.
            # 모듈 레벨 상수를 직접 변경하지 않고 로컬 변수로 섀도잉하여 스레드 안전성 보장.
            if _regime == "TREND":
                _effective_buffer_pct   = PRICE_BUFFER_PCT_TREND    # 0.2% (공격적)
                _effective_pyramid_max  = PYRAMID_MAX_ADDS           # 2회 (Phase 6 원본)
            else:  # CHOP
                _effective_buffer_pct   = PRICE_BUFFER_PCT_CHOP     # 0.5% (보수적)
                _effective_pyramid_max  = PYRAMID_MAX_ADDS_CHOP     # 0회 (전면 봉쇄)
                logger.info(
                    "🌀 [REGIME/CHOP] 횡보 국면 파라미터 적용: 버퍼=%.1f%%, 불타기 최대=%d회",
                    float(_effective_buffer_pct) * 100, _effective_pyramid_max,
                )

            # PRICE_BUFFER_PCT 로컬 별칭 (기존 하위 로직과 동일 변수명 호환)
            PRICE_BUFFER_PCT = _effective_buffer_pct

            
            # ── [가드 범위 한정] 역시그널 청산(스위칭)은 단가 가드 면제 ──────────────
            # 이 단가 버퍼 가드는 '같은 방향 추가 진입(피라미딩)'에서 평단가를 방어하기
            # 위한 것이다. 그러나 아래 두 경우는 기존 포지션을 닫는 '청산' 주문이므로
            # 유리한 단가 조건으로 막아서는 안 된다 (리스크 관리용 청산이 차단되는 버그):
            #   • action=SELL & position=LONG  → 롱 청산
            #   • action=BUY  & position=SHORT → 숏 청산
            _is_reverse_exit = (
                (action == "SELL" and current_position == "LONG")
                or (action == "BUY" and current_position == "SHORT")
            )

            # 최근 5분 이내 동일 방향 거래 이력 조회
            cooldown_cutoff = now_utc - timedelta(minutes=COOLDOWN_MINUTES)
            recent_same_side = session.query(TradeHistory).filter(
                TradeHistory.symbol == symbol, TradeHistory.side == action, TradeHistory.timestamp >= cooldown_cutoff
            ).order_by(desc(TradeHistory.timestamp)).first()

            if recent_same_side is not None and not _is_reverse_exit:
                last_price = Decimal(str(recent_same_side.price))
                
                if action == "SELL":
                    # 숏 추가 진입: 현재가가 직전 체결가보다 PRICE_BUFFER_PCT 이상 높아야 유리한 평단가 방어 가능
                    required_price = last_price * (Decimal("1") + PRICE_BUFFER_PCT)
                    if current_close < required_price:
                        logger.warning(f"⏳ [피라미딩 가드] {action} 스킵: 현재가({current_close:.2f})가 조건({required_price:.2f})에 미달하여 유리한 단가가 아님.")
                        return {"status": "pyramiding_skipped"}
                elif action == "BUY":
                    # 숏 분할 청산: 현재가가 직전 체결가보다 PRICE_BUFFER_PCT 이상 낮아야 유리함
                    required_price = last_price * (Decimal("1") - PRICE_BUFFER_PCT)
                    if current_close > required_price:
                        logger.warning(f"⏳ [피라미딩 가드] {action} 스킵: 현재가({current_close:.2f})가 조건({required_price:.2f})에 미달하여 유리한 단가가 아님.")
                        return {"status": "pyramiding_skipped"}
                        
                logger.info(f"📈 [피라미딩 통과] 단가 방어 완료! {action} 연속 진입 승인 (현재가: {current_close:.2f} / 직전가: {last_price:.2f} / 가드: {float(PRICE_BUFFER_PCT)*100:.1f}%)")

            # ── [C] 🔥 불타기 엔진 (추격 매수 — BUY 시그널 + LONG 수익권 전용) ──
            # LONG 포지션이 활성화되어 있고, BUY 추가 진입 시그널이 발화되었으며,
            # 현재 수익률이 PYRAMID_PROFIT_THRESHOLD(+0.5%) 이상일 때 실행.
            # Redis 카운터로 최대 PYRAMID_MAX_ADDS(2)회 제한, 초과 시 일반 주문 로직으로 흐름 유지.
            _pyramid_executed = False
            if (
                action == "BUY"
                and current_position == "LONG"
                and entry_price is not None
                and entry_price > Decimal("0")
                and pos_contracts > Decimal("0")
            ):
                # 롱 포지션 수익률: (현재가 - 진입가) / 진입가
                _long_roi = (current_close - entry_price) / entry_price
                if _long_roi >= PYRAMID_PROFIT_THRESHOLD:
                    # ── [최적화 ②] 싱글턴 Redis로 카운터 조회 ────────────────
                    _pyr_key = f"{_PYRAMID_COUNT_KEY_PREFIX}{_normalize_symbol(symbol)}"
                    _pyr_count = 0
                    _r_pyr = _r_sync  # 모듈 레벨 싱글턴 재사용
                    try:
                        if _r_pyr is None:
                            _r_pyr = _sync_redis_lib.Redis.from_url(
                                settings.redis_url, decode_responses=True, socket_connect_timeout=2
                            )
                        _pyr_count_raw = _r_pyr.get(_pyr_key)
                        _pyr_count = int(_pyr_count_raw) if _pyr_count_raw else 0
                    except Exception as _pyr_redis_exc:
                        logger.warning(
                            "⚠️ [불타기 엔진] Redis 카운터 조회 실패 → 불타기 스킵 (안전 처리): %s",
                            _pyr_redis_exc,
                        )
                        _pyr_count = _effective_pyramid_max  # 실패 시 한도 도달로 간주 → 스킵

                    if _pyr_count < _effective_pyramid_max:
                        # 추격 매수 수량: 현재 포지션 수량의 PYRAMID_ADD_RATIO(50%)
                        _add_amount = pos_contracts * PYRAMID_ADD_RATIO
                        # Decimal 정밀 반올림 (소수점 8자리 — CCXT float 변환 안전)
                        _add_amount = _add_amount.quantize(Decimal("0.00000001"))

                        if _add_amount >= MIN_ORDER_BTC:
                            logger.info(
                                "🔥 [불타기 엔진] LONG 포지션 ROI=%.2f%% (≥ +0.5%%) — "
                                "추격 매수 %d/%d회: %s BTC (기존 포지션 %.4f BTC의 50%%)",
                                float(_long_roi) * 100,
                                _pyr_count + 1, _effective_pyramid_max,
                                _add_amount, float(pos_contracts),
                            )
                            _pyr_result: OrderResult = _execute_order_pipeline(
                                exchange=exchange,
                                symbol=symbol,
                                side="BUY",
                                amount=_add_amount,
                                trigger_type=f"PYRAMID_ADD_{_pyr_count + 1}",
                                fallback_price=current_close,
                                confidence=confidence,
                                usdt_balance=usdt_balance,
                            )
                            # 불타기 이력 저장
                            _pyr_rec_amount = (
                                _pyr_result["filled_amount"]
                                if _pyr_result["filled_amount"] > Decimal("0")
                                else _add_amount
                            )
                            _run_async_safe(_async_save_trade_history(
                                timestamp=datetime.now(timezone.utc),
                                symbol=symbol,
                                side="BUY",
                                price=_pyr_result["filled_price"],
                                amount=_pyr_rec_amount,
                                status=_pyr_result["status"],
                            ))
                            # Redis 카운터 증가 (TTL = 1시간)
                            try:
                                _r_pyr.incr(_pyr_key)
                                _r_pyr.expire(_pyr_key, _PYRAMID_COUNT_TTL_SEC)
                            except Exception as _pyr_cnt_exc:
                                logger.warning(
                                    "⚠️ [불타기 엔진] Redis 카운터 증가 실패 (이력은 저장됨): %s",
                                    _pyr_cnt_exc,
                                )
                            notifier.send_message(
                                f"🔥 <b>[PYRAMID_ADD_{_pyr_count + 1}] 불타기 추격 매수 완료!</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"• <b>심볼:</b> <code>{symbol}</code>\n"
                                f"• <b>현재 ROI:</b> <code>{float(_long_roi)*100:+.2f}%</code>\n"
                                f"• <b>추격 수량:</b> <code>{float(_pyr_rec_amount):.4f} BTC</code>\n"
                                f"• <b>체결가:</b> <code>${float(_pyr_result['filled_price']):,.2f}</code>\n"
                                f"• <b>횟수:</b> <code>{_pyr_count + 1}/{_effective_pyramid_max}회</code>\n"
                                f"• <b>상태:</b> <code>{_pyr_result['status']}</code>\n"
                                f"━━━━━━━━━━━━━━━━━━━━"
                            )
                            _pyramid_executed = True
                            return {
                                "status": _pyr_result["status"].lower(),
                                "order_id": _pyr_result["order_id"],
                                "trigger_type": f"PYRAMID_ADD_{_pyr_count + 1}",
                                "filled_amount": str(_pyr_result["filled_amount"]),
                                "filled_price":  str(_pyr_result["filled_price"]),
                            }
                        else:
                            logger.warning(
                                "⚠️ [불타기 엔진] 추격 매수 수량 부족 → 불타기 스킵 "
                                "(계산 수량=%s < MIN=%s)",
                                _add_amount, MIN_ORDER_BTC,
                            )
                    else:
                        logger.info(
                            "🔒 [불타기 엔진] 최대 횟수(%d회) 도달 → 추격 매수 한도 초과 스킵",
                            _effective_pyramid_max,
                        )
                else:
                    logger.debug(
                        "⏸️ [불타기 엔진] LONG ROI=%.2f%% < +0.5%% — 수익권 미달, 불타기 보류",
                        float(_long_roi) * 100,
                    )

            # 7. ⚡ 주문 분기점 정의 및 동적 주문 수량 계산 (Symmetric LONG & SHORT Pipeline)
            # RISK_FACTOR는 모듈 상단에서 .env(settings.risk_factor) 기반으로 정의됨 (위 참조)
            _step = _get_lot_size_step(exchange, symbol, fallback_step="0.001")
            
            # 0 나눗셈 및 math.isnan 방어 가드
            if current_close is None or current_close <= Decimal("0") or math.isnan(float(current_close)):
                logger.error("❌ [매매 파이프라인] 현재가(current_close)가 비정상(0, None 또는 NaN)이므로 주문을 생성할 수 없습니다.")
                return {"status": "invalid_current_close"}

            # 가용 마진(Available_Margin) 안전 검증
            if usdt_free is None or usdt_free < Decimal("0") or math.isnan(float(usdt_free)):
                logger.error("❌ [매매 파이프라인] 가용 마진(usdt_free)이 비정상(None 또는 NaN)이므로 주문을 생성할 수 없습니다.")
                return {"status": "invalid_usdt_free"}

            calculated_amount = Decimal("0")
            trigger_type = "HOLD"

            if action == "SELL":
                if current_position == "LONG":
                    # 롱 포지션 청산 (스위칭)
                    trigger_type = "REVERSE_SWITCH_EXIT_LONG"
                    calculated_amount = pos_contracts
                    logger.info(f"💰 [자산 배분 - 롱 청산] API 포지션 절대 수량 기준 전량 청산: {calculated_amount} BTC")
                elif current_position == "FLAT":
                    # 신규 숏 진입
                    trigger_type = "SNIPER_SHORT_ENTRY"
                    raw_amount = (usdt_free * RISK_FACTOR) / current_close
                    # 0.001 미만으로 내려앉지 않도록 최소 안전장치 (최소 수량 하한선 0.001 BTC 설정)
                    if raw_amount < Decimal("0.001"):
                        logger.warning(
                            "⚠️ [수량 산출 - 숏 진입] 계산된 수량(%s BTC)이 최소 하한선 0.001 BTC 미만이므로, 안전장치를 가동하여 0.001 BTC로 상향 조정합니다.",
                            raw_amount
                        )
                        raw_amount = Decimal("0.001")
                    
                    # [v9.2] 신규 숏 진입(SELL) 시 Peak ROI 레지스터를 0으로 초기화
                    # — 이전 포지션의 잔류 peak 값이 새 포지션 판단에 오염되는 것을 차단
                    # 🔑 정규화된 키로 초기화하여 바이낸스 raw 심볼과의 KeyMismatch 방지
                    _peak_roi_register[_normalize_symbol(symbol)] = Decimal("0")
                    logger.info(
                        "🔄 [TRAILING_STOP] 새 숏 진입 — Peak ROI 레지스터 초기화: symbol=%s (key=%s)",
                        symbol, _normalize_symbol(symbol),
                    )

                    # 거래소 stepSize 버림 처리
                    quantized = _quantize_amount(raw_amount, _step, min_order=Decimal("0.001"))
                    calculated_amount = quantized if quantized is not None else Decimal("0")
                    logger.info(
                        f"💰 [자산 배분 - 숏 진입] 가용 USDT: {usdt_free} -> 진입 목표 (10%): {usdt_free * RISK_FACTOR} USDT -> "
                        f"계산 수량: {raw_amount} BTC -> stepSize({_step}) 적용 최종 수량: {calculated_amount} BTC"
                    )
                else:
                    trigger_type = "ALREADY_IN_SHORT_HOLD"
                    calculated_amount = Decimal("0")
                    logger.info("⏸️  [자산 배분 - 숏 진입] 이미 숏 포지션 보유 중이므로 신규 진입을 무시합니다.")

            elif action == "BUY":
                if current_position == "SHORT":
                    # 숏 포지션 청산 (스위칭)
                    trigger_type = "REVERSE_SWITCH_EXIT_SHORT"
                    calculated_amount = pos_contracts
                    logger.info(f"💰 [자산 배분 - 숏 청산] API 포지션 절대 수량 기준 전량 청산: {calculated_amount} BTC")
                elif current_position == "FLAT":
                    # 신규 롱 진입
                    trigger_type = "SNIPER_LONG_ENTRY"
                    raw_amount = (usdt_free * RISK_FACTOR) / current_close
                    # 0.001 미만으로 내려앉지 않도록 최소 안전장치 (최소 수량 하한선 0.001 BTC 설정)
                    if raw_amount < Decimal("0.001"):
                        logger.warning(
                            "⚠️ [수량 산출 - 롱 진입] 계산된 수량(%s BTC)이 최소 하한선 0.001 BTC 미만이므로, 안전장치를 가동하여 0.001 BTC로 상향 조정합니다.",
                            raw_amount
                        )
                        raw_amount = Decimal("0.001")
                    
                    # [Phase 6] 신규 LONG 진입(BUY) 시 불타기 카운터 Redis 리셋
                    # [최적화 ②] 싱글턴 클라이언트 재사용
                    try:
                        _r_reset = _r_sync if _r_sync is not None else _sync_redis_lib.Redis.from_url(
                            settings.redis_url, decode_responses=True, socket_connect_timeout=2
                        )
                        _r_reset.delete(f"{_PYRAMID_COUNT_KEY_PREFIX}{_normalize_symbol(symbol)}")
                        logger.info(
                            "🔄 [불타기 엔진] 신규 LONG 진입 — 불타기 카운터 초기화: %s",
                            _normalize_symbol(symbol),
                        )
                    except Exception as _reset_exc:
                        logger.warning("⚠️ [불타기 엔진] 카운터 초기화 실패 (무시): %s", _reset_exc)

                    # 거래소 stepSize 버림 처리
                    quantized = _quantize_amount(raw_amount, _step, min_order=Decimal("0.001"))
                    calculated_amount = quantized if quantized is not None else Decimal("0")
                    logger.info(
                        f"💰 [자산 배분 - 롱 진입] 가용 USDT: {usdt_free} -> 진입 목표 (10%): {usdt_free * RISK_FACTOR} USDT -> "
                        f"계산 수량: {raw_amount} BTC -> stepSize({_step}) 적용 최종 수량: {calculated_amount} BTC"
                    )
                else:
                    trigger_type = "ALREADY_IN_LONG_HOLD"
                    calculated_amount = Decimal("0")
                    logger.info("⏸️  [자산 배분 - 롱 진입] 이미 롱 포지션 보유 중이므로 신규 진입을 무시합니다.")

            # 방어적 검증 (Short-circuit): 수량 부족 시 주문 취소 및 조기 리턴 (최소 0.001 BTC)
            if calculated_amount <= Decimal("0") or calculated_amount < Decimal("0.001"):
                logger.warning(
                    f"⏸️  [{trigger_type}] 계산된 주문 수량이 부족하여 주문 생략: "
                    f"수량={calculated_amount}, 최소 필요=0.001"
                )
                return {"status": "insufficient_calculated_amount"}

            # ── [v5.0 방화벽 A] 자본 방화벽 — 신규 진입 주문에만 적용 ──────────────────
            # 청산(EXIT) 및 피라미딩 주문은 이미 위 불타기 엔진에서 반환됐으므로 여기서는
            # SNIPER_LONG_ENTRY / SNIPER_SHORT_ENTRY 케이스만 도달합니다.
            if trigger_type in ("SNIPER_LONG_ENTRY", "SNIPER_SHORT_ENTRY"):
                _order_usdt_val = calculated_amount * current_close
                if not _check_capital_guard(
                    symbol=symbol,
                    order_usdt_value=_order_usdt_val,
                    usdt_free=usdt_free,
                    max_pct=MAX_CAPITAL_PER_SYMBOL_PCT,
                ):
                    notifier.send_message(
                        f"🚨 <b>[CAPITAL_GUARD] 자본 방화벽 작동 — 주문 거부</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>심볼:</b> <code>{symbol}</code>\n"
                        f"• <b>트리거:</b> <code>{trigger_type}</code>\n"
                        f"• <b>주문액:</b> <code>${float(_order_usdt_val):,.2f} USDT</code>\n"
                        f"• <b>한도:</b> <code>${float(usdt_free * MAX_CAPITAL_PER_SYMBOL_PCT):,.2f} USDT "
                        f"(가용 마진 {float(MAX_CAPITAL_PER_SYMBOL_PCT)*100:.0f}%)</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━"
                    )
                    return {
                        "status": "capital_guard_rejected",
                        "trigger_type": trigger_type,
                        "order_usdt_value": str(_order_usdt_val),
                    }

                # ── [v5.0 방화벽 B] 슬리페이지 가드 ──────────────────────────────────
                if not _check_slippage_guard(
                    exchange=exchange,
                    symbol=symbol,
                    ref_price=current_close,
                    side=action,
                    limit_pct=SLIPPAGE_GUARD_PCT,
                ):
                    notifier.send_message(
                        f"🚨 <b>[SLIPPAGE_GUARD] 슬리페이지 초과 — 주문 REJECTED</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>심볼:</b> <code>{symbol}</code>\n"
                        f"• <b>트리거:</b> <code>{trigger_type}</code>\n"
                        f"• <b>기준가:</b> <code>${float(current_close):,.2f}</code>\n"
                        f"• <b>허용 슬리페이지:</b> <code>{float(SLIPPAGE_GUARD_PCT)*100:.1f}%</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━"
                    )
                    return {
                        "status": "slippage_guard_rejected",
                        "trigger_type": trigger_type,
                        "ref_price": str(current_close),
                    }

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

            # 이력 저장 — [교착 방지] _run_async_safe()로 스레드 격리 실행
            # PARTIALLY_FILLED 시 실 체결 수량, 그 외 미체결(CANCELLED/FAILED) 시 요청 수량 기준 기록
            record_amount = (
                exec_result["filled_amount"]
                if exec_result["filled_amount"] > Decimal("0")
                else calculated_amount
            )
            _run_async_safe(_async_save_trade_history(
                timestamp=now_utc,
                symbol=symbol,
                side=action,
                price=exec_result["filled_price"],
                amount=record_amount,
                status=exec_result["status"],
            ))
            logger.info(
                "🗄️ [%s] DB 이력 저장 완료 (스레드 격리 비동기): status=%s, filled_amount=%s, order_id=%s",
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
    finally:
        # ── [결함 #4 수정] 분산 락 해제 — 조기 return/예외와 무관하게 항상 실행 ──
        # 우리가 실제로 점유한 락(_lock_held)만 삭제한다.
        # TTL(30초)이 만료되기 전 다음 트리거가 즉시 매매 로직에 진입할 수 있도록
        # 정상/에러 종료 모두에서 락을 즉시 반납한다.
        # (self.retry()는 내부적으로 예외를 재발생시키므로 이 finally는 그 직전에 실행됨)
        if _lock_held and _r_lock is not None:
            try:
                _r_lock.delete(_lock_key)
                logger.debug("🔓 [중복 주문 방지] '%s' 심볼 락 해제 완료", symbol)
            except Exception as _unlock_exc:
                logger.warning(
                    "⚠️ [중복 주문 방지] 락 해제 실패 (TTL %ds 후 자동 만료 대기): %s",
                    _ORDER_DEDUP_LOCK_TTL_SEC, _unlock_exc,
                )


# ⏱️ NTP 시간 동기화 및 스켈레톤 함수 유지
@celery_app.task(name="worker.tasks.check_time_sync_task", queue="default")
def check_time_sync_task():
    return {"ntp_drift_ms": round(check_ntp_drift(), 1)}




# ─────────────────────────────────────────────
# 📥 [8차 확장] 텔레그램 실시간 /status 명령어 및 일일 결산 스케줄러
# ─────────────────────────────────────────────

def _send_status_brief():
    """
    📊 실시간 시스템 관제 요약 브리핑을 조립하여 텔레그램으로 전송합니다.
    (비동기 DB 조회 원천 제거, 100% 동기식 API 기반 단일화 완료)
    """
    try:
        # get_exchange()가 settings.exchange_sandbox 기준으로 데모/실전을 이미 결정하므로
        # 별도의 set_sandbox_mode 토글은 불필요하다. (과거 존재하지 않는 속성
        # settings.BINANCE_SANDBOX_MODE를 참조하던 죽은 코드를 제거)
        exchange = get_exchange()

        total_assets, usdt_free = _get_futures_margin_balance(exchange)
        
        symbol = "BTC/USDT"
        
        # ── 1. 시장 지표 조회 (동기식 CCXT API 활용) ──────────────────────────────────
        current_close = Decimal("0")
        rsi_val = 0.0
        bb_upper_val = 0.0
        bb_lower_val = 0.0
        sma_val = 0.0

        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='15m', limit=30)
            if ohlcv and len(ohlcv) >= 20:
                closes = [c[4] for c in ohlcv]
                current_close = Decimal(str(closes[-1]))
                
                # SMA 20 & BB 20
                recent_20 = closes[-20:]
                sma = sum(recent_20) / 20.0
                sma_val = float(sma)
                variance = sum((x - sma) ** 2 for x in recent_20) / 20.0
                std_dev = variance ** 0.5
                bb_upper_val = float(sma + 2 * std_dev)
                bb_lower_val = float(sma - 2 * std_dev)
                
                # 간단한 RMA 기반 RSI 14 산출
                if len(closes) >= 15:
                    recent_15 = closes[-15:]
                    gains, losses = [], []
                    for i in range(1, 15):
                        change = recent_15[i] - recent_15[i-1]
                        if change > 0:
                            gains.append(change)
                            losses.append(0)
                        else:
                            gains.append(0)
                            losses.append(abs(change))
                    avg_gain = sum(gains) / 14.0
                    avg_loss = sum(losses) / 14.0
                    if avg_loss == 0:
                        rsi_val = 100.0
                    else:
                        rs = avg_gain / avg_loss
                        rsi_val = float(100.0 - (100.0 / (1.0 + rs)))
            elif ohlcv:
                current_close = Decimal(str(ohlcv[-1][4]))
        except Exception as e:
            logger.warning(f"⚠️ CCXT 지표 계산 실패 (진행은 계속함): {e}")

        # ── 2. 단방향(One-Way) 전용 포지션 파싱 ───────────────────────────────────────
        ccxt_pos = None
        _one_way_pos_amt = 0.0
        try:
            if exchange.has.get('fetchPositions'):
                _raw_positions = exchange.fetch_positions([symbol])
                for _p in _raw_positions:
                    _raw_info_ow = _p.get('info', {})
                    _pa_raw = _raw_info_ow.get('positionAmt', '0')
                    try:
                        _pa_float = float(_pa_raw)
                    except (TypeError, ValueError):
                        _pa_float = 0.0
                    
                    if abs(_pa_float) > 0.0:
                        ccxt_pos = _p
                        _one_way_pos_amt = _pa_float
                        break
        except Exception as _pos_exc:
            logger.warning(f"⚠️ CCXT 포지션 조회 실패: {_pos_exc}")

        current_position = "HOLD"
        entry_price = None
        pos_duration_str = "-"
        pos_pnl_str = " (포지션 없음)"

        if ccxt_pos:
            if _one_way_pos_amt > 0:
                current_position = "LONG"
            elif _one_way_pos_amt < 0:
                current_position = "SHORT"

            entry_price = Decimal(str(ccxt_pos.get('entryPrice', 0) or 0))
            pos_amount  = Decimal(str(abs(_one_way_pos_amt)))
            
            _unrealized_raw = float(ccxt_pos.get('unrealizedPnl', 0.0) or 0.0)
            if ccxt_pos.get('percentage') is not None:
                pos_return = float(ccxt_pos['percentage'])
            elif entry_price is not None and entry_price > Decimal("0") and pos_amount > Decimal("0"):
                _pos_value = float(entry_price * pos_amount)
                pos_return = (_unrealized_raw / _pos_value * 100) if _pos_value > 0 else 0.0
            else:
                pos_return = 0.0
            
            pos_pnl_str = f" ({pos_return:+.2f}%)"
            
            # DB 조회가 없으므로 바이낸스 updateTime을 통해 보유 기간을 간접 추정
            try:
                _update_time_ms = int(ccxt_pos.get('info', {}).get('updateTime', 0))
                if _update_time_ms > 0:
                    entry_ts = datetime.fromtimestamp(_update_time_ms / 1000.0, timezone.utc)
                    minutes_held = (datetime.now(timezone.utc) - entry_ts).total_seconds() / 60.0
                    # updateTime은 펀딩비 정산 등에도 갱신될 수 있으므로 (추정) 표기
                    pos_duration_str = f"{minutes_held:.0f}분 보유 (추정)"
            except Exception:
                pass

        # ── 3. 24H 실현 손익 및 Income API 연동 ──────────────────────────────────────
        _24h_realized_pnl = Decimal("0")
        _income_commission  = Decimal("0")
        _income_funding_fee = Decimal("0")
        _income_api_ok      = False
        
        cutoff_24h = datetime.now(timezone.utc) - timedelta(days=1)
        
        try:
            _income_start_ms = int(cutoff_24h.timestamp() * 1000)
            # 최근 1000건 확보로 PNL, COMMISSION, FUNDING_FEE 모두 커버
            _income_raw = exchange.fapiPrivateGetIncome(params={
                "startTime": _income_start_ms,
                "limit":     1000,
            })
            
            if isinstance(_income_raw, list):
                for _inc in _income_raw:
                    try:
                        _inc_time = int(_inc.get("time", 0) or 0)
                        if _inc_time < _income_start_ms:
                            continue
                            
                        _inc_type   = str(_inc.get("incomeType", "") or "").upper()
                        _inc_income = _inc.get("income", None)
                        if _inc_income is None:
                            continue
                        _inc_amount = Decimal(str(_inc_income))

                        if _inc_type == "COMMISSION":
                            _income_commission += _inc_amount
                        elif _inc_type == "FUNDING_FEE":
                            _income_funding_fee += _inc_amount
                        elif _inc_type == "REALIZED_PNL":
                            _24h_realized_pnl += _inc_amount
                    except Exception:
                        continue
                _income_api_ok = True
                logger.info(f"✅ [Income API] 조회 완료 — PNL={_24h_realized_pnl}, COMM={_income_commission}")
            else:
                logger.warning(f"⚠️ [Income API] list 아님, type={type(_income_raw)}")
        except Exception as _income_exc:
            logger.warning(f"⚠️ [Income API] 조회 실패: {_income_exc}")

        _net_pnl_24h = _24h_realized_pnl + _income_commission + _income_funding_fee
        
        _trading_pnl_str  = f"{_24h_realized_pnl:+,.2f} USDT"
        _commission_str   = f"{_income_commission:+,.2f} USDT" if _income_api_ok else f"{float(_income_commission):.2f} (조회실패)"
        _funding_fee_str  = f"{_income_funding_fee:+,.2f} USDT" if _income_api_ok else f"{float(_income_funding_fee):.2f} (조회실패)"
        _net_pnl_str = f"{_net_pnl_24h:+,.2f} USDT"

        # 포지션 이모지
        if current_position == "SHORT":
            pos_emoji = "🔴 SHORT"
        elif current_position == "LONG":
            pos_emoji = "🟢 LONG"
        else:
            pos_emoji = "🟢 HOLD"
            pos_pnl_str = " (포지션 없음)"

        kst_tz = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst_tz).strftime("%Y-%m-%d %H:%M:%S KST")

        # ── 4. 브리핑 포맷 조립 ─────────────────────────────────────────────────────
        brief_lines = [
            "📊 <b>[QuantFlow 실시간 관제 브리핑]</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"• 🤖 <b>엔진 상태:</b> <code>RUNNING</code>",
            f"• ⏰ <b>관제 시각:</b> <code>{now_kst}</code>",
            "",
            "📦 <b>현재 유지 포지션</b>",
            "────────────────────",
            f"• <b>상태:</b> <b>{pos_emoji}</b>{pos_pnl_str}",
            f"• <b>평단가:</b> <code>${float(entry_price or 0):,.2f} USDT</code>" if entry_price is not None and entry_price > Decimal("0") else "• <b>평단가:</b> <code>-</code>",
            f"• <b>경과 시간:</b> <code>{pos_duration_str}</code>",
            "",
            "💰 <b>실시간 자산 잔고 (선물 지갑)</b>",
            "────────────────────",
            f"• <b>가용 잔고 (Free):</b> <code>${float(usdt_free):,.2f} USDT</code>",
            f"• <b>총 자산 (Total):</b> <code>${float(total_assets):,.2f} USDT</code>",
            f"• <b>24H 정산 손익:</b> <code>{_net_pnl_str}</code>",
            f"  └ 순수 매매손익: <code>{_trading_pnl_str}</code>",
            f"  └ 누적 수수료:  <code>{_commission_str}</code>",
            f"  └ 누적 펀딩비:  <code>{_funding_fee_str}</code>",
            "",
            "📈 <b>실시간 시장 지표 (API 15m)</b>",
            "────────────────────",
            f"• <b>현재가 (Close):</b> <code>${float(current_close):,.2f}</code>",
            f"• <b>RSI (14주기):</b> <code>{rsi_val:.2f}</code>",
            f"• <b>볼린저 밴드 상단:</b> <code>${bb_upper_val:,.2f}</code>",
            f"• <b>볼린저 밴드 하단:</b> <code>${bb_lower_val:,.2f}</code>",
            f"• <b>SMA (20주기):</b> <code>${sma_val:,.2f}</code>",
            "━━━━━━━━━━━━━━━━━━━━"
        ]
        
        notifier.send_message("\n".join(brief_lines))
        logger.info("✅ 텔레그램 '/status' 브리핑 발송 완료 (DB 종속성 100% 제거)")
        
    except Exception as e:
        logger.error(f"❌ _send_status_brief() 중 에러 발생: {e}", exc_info=True)
        # 텔레그램 HTML 파싱 에러 방어: str(e)에 포함된 '<', '>' 등의 문자를 이스케이프 처리
        _safe_err = str(e).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        notifier.send_message(f"❌ <b>[QuantFlow 오류]</b>\n실시간 상태 브리핑 작성 중 실패했습니다: <code>{_safe_err}</code>")

import unicodedata
import requests

def handle_telegram_command(command: str) -> str:
    """
    텔레그램 챗봇으로부터 수신된 명령어를 처리하고 Redis 상태를 제어하며,
    비동기 세션 데드락 방지를 위해 독립적인 HTTP 소켓(requests)으로 응답을 즉시 선제 발송합니다.
    """
    import redis
    # 싱글턴 클라이언트가 준비되어 있으면 재사용
    r = _r_sync if _r_sync is not None else redis.Redis.from_url(settings.redis_url, decode_responses=True)
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


@celery_app.task(
    name="worker.tasks.telegram_command_listener_task",
    queue="listener",         # 매매/데이터 큐와 워커 슬롯 공유 완전 차단 — 전용 큐 격리
    soft_time_limit=25,       # 25초 초과 시 SoftTimeLimitExceeded → 즉시 응답 포기 및 종료
    time_limit=28,            # 28초 절대 하드 킬 (30초 Beat 스케줄 주기 대비 2초 안전 마진)
)
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

    # [최적화 ②] 싱글턴 클라이언트 재사용
    r = _r_sync if _r_sync is not None else redis.Redis.from_url(settings.redis_url, decode_responses=True)
    
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

    [v9.3 버그픽스 항목]
    1. CCXT fetch_positions 교차 검증 추가 — DB 단독 의존 탈피
    2. SHORT 포지션 판별 추가 — 기존에는 LONG(BUY)만 처리하여 SHORT를 영구 FLAT 출력
    3. _normalize_symbol 전면 적용 — 심볼 포맷 불일치(KeyMismatch) 원천 차단
    4. PnL 계산 로직을 Short 전략 기준으로 수정 — (진입가 - 청산가) × 수량
    5. 미실현 손익(Unrealized PnL) 별도 표시 추가
    6. pos_duration_str 출력 조건 수정 — entry_price 조건 대신 문자열 직접 체크
    """
    logger.info("📊 [일간 결산] generate_daily_report_task 가동")
    
    try:
        kst_tz = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst_tz)
        
        # 🔑 [버그픽스] KST→UTC 변환 오차(9시간 시프트)로 인해 자정 직후 체결 내역이
        #   누락되는 버그를 방지하기 위해, timezone-naive KST 자정 기준점을 직접 생성하여
        #   DB의 naive timestamp 컬럼과 직접 비교합니다.
        start_of_day_naive = datetime(now_kst.year, now_kst.month, now_kst.day, 0, 0, 0)
        
        # ── 실시간 자산 및 시세 조회 ──────────────────────────────────────────
        exchange = get_exchange()
        total_assets_val, usdt_free_val = _get_futures_margin_balance(exchange)
        
        # 🔑 [결함 #3 해결] VS Code undefined btc_balance 경고 제거 및 실시간 현물 BTC 조회
        btc_balance = Decimal("0")
        try:
            spot_bal = exchange.fetch_balance({"type": "spot"})
            if spot_bal:
                if "total" in spot_bal and "BTC" in spot_bal["total"]:
                    btc_balance = Decimal(str(spot_bal["total"]["BTC"]))
                elif "BTC" in spot_bal:
                    if isinstance(spot_bal["BTC"], dict):
                        btc_balance = Decimal(str(spot_bal["BTC"].get("total", 0.0)))
                    else:
                        btc_balance = Decimal(str(spot_bal["BTC"]))
        except Exception as spot_exc:
            logger.warning(f"⚠️ [일일결산] 현물 BTC 잔고 조회 실패 (0.0 처리): {spot_exc}")

        symbol = "BTC/USDT"
        # 🔑 정규화된 심볼 (바이낸스 raw 포맷 "BTCUSDT" 매칭용 — 로그 추적에 활용)
        symbol_normalized = _normalize_symbol(symbol)

        # ── [교착 방지 ②] generate_daily_report 비동기 쿼리 사전 실행 ─────────────
        # for session in _get_sync_session() 외부에서 _run_async_safe()로 미리 조회.
        # 세션 내부에서 asyncio.run() 호출 시 커넥션 경합 Deadlock 유발 원천 차단.
        async def _combined_report_query():
            return await asyncio.gather(
                _async_query_today_trades(symbol, start_of_day_naive),
                _async_query_all_filled_sells(symbol)
            )

        today_trades, all_filled_sells_for_pnl = _run_async_safe(_combined_report_query())

        for session in _get_sync_session():
            latest = session.query(MarketData).filter(
                MarketData.symbol == symbol
            ).order_by(desc(MarketData.timestamp)).first()
            
            current_close = Decimal(str(latest.close)) if latest else Decimal("0")
            total_assets = total_assets_val
            usdt_balance = usdt_free_val
            
            # ── [STEP 1] CCXT positionAmt 기반 원격 API 단일 포지션 판정 파이프라인 ───
            # 🔑 [v10 전면 고도화] DB Fallback을 완전히 배제하고, 원격 API의 positionAmt 부호만을
            #   절대 진실로 삼아 포지션을 판정합니다. DB 이력 역산 방식은 청산 이력 누락 시
            #   과거 포지션(SHORT)이 잔류하는 동기화 유실 결함을 유발하므로 완전 소각합니다.
            #   positionAmt > 0 → LONG / < 0 → SHORT / == 0 또는 데이터 없음 → FLAT
            current_position  = "FLAT"
            pos_emoji         = "⚪ FLAT"
            entry_price       = None
            pos_pnl_str       = ""
            pos_duration_str  = "-"
            unrealized_pnl    = Decimal("0")
            unrealized_pnl_str = ""
            ccxt_pos          = None
            _pos_amt_float    = 0.0

            try:
                if exchange.has.get("fetchPositions"):
                    _raw_positions = exchange.fetch_positions([symbol])
                    for _p in _raw_positions:
                        # positionAmt: 바이낸스 단방향(One-Way) 모드의 실제 보유량 부호값
                        _raw_info     = _p.get("info", {})
                        _pa_raw       = _raw_info.get("positionAmt", "0")
                        try:
                            _pa_float = float(_pa_raw)
                        except (TypeError, ValueError):
                            _pa_float = 0.0

                        if abs(_pa_float) > 0.0:
                            ccxt_pos       = _p
                            _pos_amt_float = _pa_float
                            break  # 활성 포지션 확보 즉시 탈출

                logger.info(
                    "📊 [일일결산] CCXT positionAmt 조회 완료 — positionAmt=%.6f, ccxt_pos=%s",
                    _pos_amt_float, "확보" if ccxt_pos else "없음(FLAT)",
                )
            except Exception as _pos_exc:
                logger.warning(
                    "⚠️ [일일결산] CCXT fetch_positions 실패 → FLAT으로 안전 처리 (DB Fallback 없음): %s",
                    _pos_exc,
                )

            # ── positionAmt 부호 기반 포지션 방향 확정 ─────────────────────────
            if ccxt_pos is not None and _pos_amt_float > 0.0:
                current_position = "LONG"
                pos_emoji        = "🟢 LONG"
            elif ccxt_pos is not None and _pos_amt_float < 0.0:
                current_position = "SHORT"
                pos_emoji        = "🔴 SHORT"
            else:
                # positionAmt == 0 이거나 데이터 자체가 없음 → 완전 FLAT
                current_position   = "FLAT"
                pos_emoji          = "⚪ FLAT"
                entry_price        = None
                pos_pnl_str        = ""
                pos_duration_str   = "-"
                unrealized_pnl     = Decimal("0")
                unrealized_pnl_str = ""
                logger.info("📊 [일일결산] 포지션 없음(FLAT) 확정 — 포지션 상세 필드 초기화 완료")

            # ── 활성 포지션(LONG/SHORT)일 때만 상세 지표 파싱 ──────────────────
            if current_position in ("LONG", "SHORT") and ccxt_pos is not None:
                entry_price     = Decimal(str(ccxt_pos.get("entryPrice", 0) or 0))
                pos_amount      = Decimal(str(abs(_pos_amt_float)))  # 절대값으로 수량 확보
                _unrealized_raw = ccxt_pos.get("unrealizedPnl", 0.0) or 0.0
                unrealized_pnl  = Decimal(str(_unrealized_raw))

                # 수익률 계산 (거래소 percentage 우선, 없으면 unrealizedPnl/포지션가치 비율)
                if ccxt_pos.get("percentage") is not None:
                    pos_return = float(ccxt_pos["percentage"])
                elif entry_price > Decimal("0") and pos_amount > Decimal("0"):
                    pos_value  = float(entry_price * pos_amount)
                    pos_return = (float(_unrealized_raw) / pos_value * 100) if pos_value > 0 else 0.0
                else:
                    if current_position == "SHORT" and entry_price > Decimal("0"):
                        pos_return = float((entry_price - current_close) / entry_price * 100)
                    elif entry_price > Decimal("0"):
                        pos_return = float((current_close - entry_price) / entry_price * 100)
                    else:
                        pos_return = 0.0

                pos_pnl_str = f" ({pos_return:+.2f}%)"

                # 미실현 손익 문자열 생성
                _upnl_sign         = "+" if unrealized_pnl >= Decimal("0") else ""
                unrealized_pnl_str = f"{_upnl_sign}{float(unrealized_pnl):,.2f} USDT"

                # 경과 시간: DB 진입 이력에서 최근 체결 시각을 참조 (API에 updateTime 없을 때 보조)
                # SHORT: 가장 최근 SELL(숏 진입) 기준 / LONG: 가장 최근 BUY(롱 진입) 기준
                _entry_side_filter = "SELL" if current_position == "SHORT" else "BUY"
                _last_entry_trade  = session.query(TradeHistory).filter(
                    TradeHistory.symbol == symbol,
                    TradeHistory.side   == _entry_side_filter,
                    TradeHistory.status == "FILLED",
                ).order_by(desc(TradeHistory.timestamp)).first()

                if _last_entry_trade:
                    _ets = _last_entry_trade.timestamp
                    if _ets.tzinfo is None:
                        _ets = _ets.replace(tzinfo=timezone.utc)
                    else:
                        _ets = _ets.astimezone(timezone.utc)
                    _mins = (datetime.now(timezone.utc) - _ets).total_seconds() / 60.0
                    pos_duration_str = f"{_mins:.0f}분 보유"
                    logger.info(
                        "📊 [일일결산] 포지션 경과 시간 확정: %.0f분 (side=%s, entry_ts=%s)",
                        _mins, _entry_side_filter, _ets.isoformat(),
                    )

            # ── [STEP 2] 오늘 KST 00:00 이후 체결된 주문 스캔 ──────────────────
            # (today_trades — for session 블록 외부에서 _run_async_safe()로 사전 조회 완료.
            #  세션 내부 asyncio.run() 재호출 금지 — 커넥션 풀 경합 Deadlock 방지)
            logger.info(
                "📊 [일일결산] 오늘 거래 스캔: symbol=%s (normalized=%s), start=%s KST(naive), count=%d건",
                symbol, symbol_normalized, start_of_day_naive.isoformat(), len(today_trades),
            )

            # ── [STEP 3] 숏 전략 기반 PnL 계산 ────────────────────────────────
            # (all_filled_sells_for_pnl — for session 블록 외부에서 _run_async_safe()로 사전 조회 완료)
            # Short PnL = (숏 진입가 - 청산가) × 수량  =  (avg_sell_price - buy_price) × buy_qty

            today_filled_buys_for_pnl = [
                t for t in today_trades
                if t.side == "BUY" and t.status == "FILLED" and t.price is not None
            ]

            # 숏 진입(SELL)들을 시간 오름차순으로 정렬하여 FIFO 매칭 큐 구성
            sorted_sells = sorted(all_filled_sells_for_pnl, key=lambda x: x.timestamp)
            sell_queue = []
            for s in sorted_sells:
                if s.price is not None and s.amount is not None:
                    sell_queue.append({
                        "price": Decimal(str(s.price)),
                        "amount": Decimal(str(s.amount)),
                        "remaining": Decimal(str(s.amount)),
                        "timestamp": s.timestamp
                    })

            # 오늘 청산(BUY)들을 시간 오름차순으로 정렬
            sorted_buys = sorted(today_filled_buys_for_pnl, key=lambda x: x.timestamp)

            realized_pnl = Decimal("0")
            win_count    = 0
            loss_count   = 0

            for buy_trade in sorted_buys:
                buy_price = Decimal(str(buy_trade.price))
                buy_qty   = Decimal(str(buy_trade.amount))
                
                trade_pnl = Decimal("0")
                matched_total_qty = Decimal("0")
                
                # FIFO 방식으로 이 BUY 이전의 SELL들과 매칭
                for sell in sell_queue:
                    if buy_qty <= Decimal("0"):
                        break
                    if sell["timestamp"] > buy_trade.timestamp:
                        # BUY 시점 이후의 진입은 매칭 불가
                        continue
                    if sell["remaining"] <= Decimal("0"):
                        continue
                        
                    match_qty = min(buy_qty, sell["remaining"])
                    trade_pnl += (sell["price"] - buy_price) * match_qty
                    matched_total_qty += match_qty
                    
                    sell["remaining"] -= match_qty
                    buy_qty -= match_qty
                
                if matched_total_qty > Decimal("0"):
                    realized_pnl += trade_pnl
                    if trade_pnl > Decimal("0"):
                        win_count += 1
                    else:
                        loss_count += 1

            # ── [STEP 4] 매매 요약 집계 ─────────────────────────────────────────
            total_count    = len(today_trades)
            filled_count   = sum(1 for t in today_trades if t.status == "FILLED")
            rejected_count = sum(1 for t in today_trades if t.status == "REJECTED")
            failed_count   = sum(1 for t in today_trades if t.status == "FAILED")
            
            fill_rate_str = f"{(filled_count / total_count * 100):.1f}%" if total_count > 0 else "N/A"

            evaluated_total = win_count + loss_count
            win_rate_str    = f"{(win_count / evaluated_total * 100):.1f}%" if evaluated_total > 0 else "N/A"

            if realized_pnl > Decimal("0"):
                pnl_emoji = "📈"
                pnl_sign  = "+"
            elif realized_pnl < Decimal("0"):
                pnl_emoji = "📉"
                pnl_sign  = ""
            else:
                pnl_emoji = "➖"
                pnl_sign  = ""


            report_time_kst = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")

            # ── [STEP 5] 오늘 매매 타임라인 생성 ────────────────────────────────
            _dr_kst_tz = timezone(timedelta(hours=9))
            _today_filled = [t for t in today_trades if t.status == "FILLED"]
            if _today_filled:
                _dr_timeline_lines = []
                for _t in _today_filled:
                    _ts = _t.timestamp
                    if _ts.tzinfo is None:
                        _ts = _ts.replace(tzinfo=timezone.utc)
                    else:
                        _ts = _ts.astimezone(timezone.utc)
                    _ts_kst = _ts.astimezone(_dr_kst_tz)
                    _time_str = _ts_kst.strftime("%H:%M")
                    if _t.side == "SELL":
                        _side_str = "🔴 숏 진입 (or 롱 청산)"
                    else:
                        _side_str = "🟢 롱 진입 (or 숏 청산)"
                    _price_str = f"${float(_t.price):,.2f}" if _t.price else "-"
                    _qty_str   = f"{float(_t.amount):.6f}" if _t.amount else "-"
                    _dr_timeline_lines.append(
                        f"[{_time_str}] {_side_str}\n"
                        f"  ├ 체결가: <code>{_price_str}</code>\n"
                        f"  └ 수량:  <code>{_qty_str} BTC</code>"
                    )
                _daily_timeline_str = "\n".join(_dr_timeline_lines)
            else:
                _daily_timeline_str = "• 오늘 체결된 내역이 없습니다."

            # ── [STEP 6] 리포트 메시지 조립 ─────────────────────────────────────
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
                # 🔑 [버그픽스] pos_duration_str 자체의 유효성을 직접 체크
                f"• <b>경과 시간:</b> <code>{pos_duration_str}</code>" if (pos_duration_str and pos_duration_str != "-") else "• <b>경과 시간:</b> <code>-</code>",
                f"• <b>미실현 손익:</b> <code>{unrealized_pnl_str}</code>" if unrealized_pnl_str else "• <b>미실현 손익:</b> <code>-</code>",
                "",
                "📝 <b>오늘 상세 매매 타임라인</b>",
                "────────────────────",
                _daily_timeline_str,
                "",
                "📋 <b>오늘 하루 매매 요약</b>",
                "────────────────────",
                f"• <b>총 주문 시도:</b> <code>{total_count}건</code>",
                f"• <b>체결 완료(FILLED):</b> <code>{filled_count}건</code>",
                f"• <b>거부(REJECTED):</b> <code>{rejected_count}건</code>",
                f"• <b>실패(FAILED):</b> <code>{failed_count}건</code>",
                f"• <b>체결 성공률:</b> <code>{fill_rate_str}</code>",
                "",
                "🎯 <b>실현 손익 및 승률</b>  ※ Short 전략 기준",
                "────────────────────",
                f"• {pnl_emoji} <b>금일 실현 손익:</b> <code>{pnl_sign}{realized_pnl:,.2f} USDT</code>",
                f"• <b>익절(Win):</b> <code>{win_count}건</code> / <b>손절(Loss):</b> <code>{loss_count}건</code>",
                f"• <b>승률:</b> <code>{win_rate_str}</code>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>리포트 생성:</b> <code>{report_time_kst}</code>",
            ]
            
            notifier.send_message("\n".join(report_lines))
            logger.info(
                "📊 [generate_daily_report_task] 리포트 전송 완료 — 포지션=%s, PnL: %s%s USDT, 미실현: %s",
                current_position, pnl_sign, f"{realized_pnl:,.2f}", unrealized_pnl_str or "N/A",
            )

            return {
                "status":         "report_sent",
                "position":       current_position,
                "total_trades":   total_count,
                "filled_trades":  filled_count,
                "realized_pnl":   str(realized_pnl),
                "unrealized_pnl": str(unrealized_pnl),
                "win_rate":       win_rate_str,
            }

    except Exception as exc:
        logger.error(f"❌ [generate_daily_report_task] 리포트 생성 중 예외 발생: {exc}", exc_info=True)
        return {"status": "report_failed", "error": str(exc)}