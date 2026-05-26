"""
app.models.models — 시장 데이터 ORM 모델
부동소수점 오차 방지를 위해 가격/거래량은 Numeric(18, 8) 사용
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    DateTime,
    Numeric,
    Float,
    Enum as SAEnum,
    UniqueConstraint,
    Index,
)

from core.database import Base


class MarketData(Base):
    """
    거래소에서 수집한 OHLCV 캔들 데이터 + 피처 엔지니어링 지표.

    Columns:
        id        — 자동 증가 기본키
        timestamp — 캔들 시작 시각 (UTC, millisecond epoch → datetime)
        symbol    — 거래 심볼 (예: BTC/USDT)
        open      — 시가
        high      — 고가
        low       — 저가
        close     — 종가
        volume    — 거래량
        sma_20    — 20주기 단순이동평균 (초기 데이터 부족 시 NULL)
        rsi_14    — 14주기 RSI (초기 데이터 부족 시 NULL)
        bb_upper  — 볼린저밴드 상단 (초기 데이터 부족 시 NULL)
        bb_lower  — 볼린저밴드 하단 (초기 데이터 부족 시 NULL)
    """

    __tablename__ = "market_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 캔들 시작 시각 (UTC 기준)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)

    # 심볼 (예: BTC/USDT)
    symbol = Column(String(20), nullable=False, index=True)

    # ── OHLCV ─────────────────────────────────────────────────────────────
    # 부동소수점 오차 방지: Numeric(precision=18, scale=8)
    #   precision=18 → 최대 18자리 정수부
    #   scale=8      → 소수점 이하 8자리 (사토시 단위까지 표현 가능)
    open   = Column(Numeric(precision=18, scale=8), nullable=False)
    high   = Column(Numeric(precision=18, scale=8), nullable=False)
    low    = Column(Numeric(precision=18, scale=8), nullable=False)
    close  = Column(Numeric(precision=18, scale=8), nullable=False)
    volume = Column(Numeric(precision=18, scale=8), nullable=False)

    # ── 피처 엔지니어링 지표 ─────────────────────────────────────────────
    # 초기 데이터 부족(warm-up 기간) 시 NULL 허용
    sma_20   = Column(Numeric(precision=18, scale=8), nullable=True)   # 20주기 SMA
    rsi_14   = Column(Numeric(precision=18, scale=8), nullable=True)   # 14주기 RSI
    bb_upper = Column(Numeric(precision=18, scale=8), nullable=True)   # 볼린저밴드 상단
    bb_lower = Column(Numeric(precision=18, scale=8), nullable=True)   # 볼린저밴드 하단

    atr_14      = Column(Float, nullable=True)  # 14주기 ATR
    macd_line   = Column(Float, nullable=True)  # MACD 라인
    macd_signal = Column(Float, nullable=True)  # MACD 시그널
    macd_hist   = Column(Float, nullable=True)  # MACD 히스토그램

    # ── 제약 조건 ────────────────────────────────────────────────────────
    # (timestamp, symbol) 쌍은 유일 → 동일 캔들 중복 삽입 방지
    __table_args__ = (
        UniqueConstraint("timestamp", "symbol", name="uq_market_data_ts_symbol"),
        Index("ix_market_data_symbol_timestamp", "symbol", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<MarketData {self.symbol} "
            f"ts={self.timestamp.isoformat() if self.timestamp else None} "
            f"close={self.close}>"
        )


class TradeHistory(Base):
    """
    자동 매매 주문 이력.

    Columns:
        id        — 자동 증가 기본키
        timestamp — 주문 시각 (UTC)
        symbol    — 거래 심볼 (예: BTC/USDT)
        side      — 주문 방향 (BUY / SELL)
        price     — 체결 가격 (시장가 주문 시 체결 평균가)
        amount    — 주문 수량
        status    — 주문 상태 (FILLED, PARTIAL, REJECTED 등)
    """

    __tablename__ = "trade_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 주문 시각 (UTC)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)

    # 심볼 (예: BTC/USDT)
    symbol = Column(String(20), nullable=False, index=True)

    # 주문 방향 — BUY 또는 SELL 만 허용
    side = Column(SAEnum("BUY", "SELL", name="trade_side_enum"), nullable=False)

    # 체결 가격
    price = Column(Numeric(precision=18, scale=8), nullable=True)

    # 주문 수량
    amount = Column(Numeric(precision=18, scale=8), nullable=False)

    # 주문 상태
    status = Column(String(20), nullable=False, default="PENDING")

    # ── 제약 조건 ────────────────────────────────────────────────────────
    # 쿨다운 조회 최적화: (symbol, side, timestamp) 복합 인덱스
    __table_args__ = (
        Index("ix_trade_history_cooldown", "symbol", "side", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<TradeHistory {self.side} {self.amount} {self.symbol} "
            f"@ {self.price} [{self.status}]>"
        )

