"""
worker.indicators — 다중 기술적 지표 벡터 연산 모듈 (TA-Lib 無의존)

Phase 4: 순수 pandas/numpy 기반 12종 기술적 지표 피처 엔지니어링 파이프라인.
OHLCV 시계열 데이터를 입력받아 추세·모멘텀·변동성·거래량 카테고리의
입체적 피처 벡터를 인메모리 벡터 연산으로 생성합니다.

지표 카테고리:
  [추세 Trend]        EMA(9,20,50), EMA 정배열/역배열, MACD(12,26,9)
  [모멘텀 Momentum]   RSI(14), Stochastic Oscillator(%K,%D)
  [변동성 Volatility] Bollinger Bands(20,2), SMA(20)
  [거래량 Volume]     Volume MA Ratio(20)
"""

import logging
from typing import List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── ML 모델 피처 입력 순서 (학습/추론 시 동일 순서 엄수) ──────────────────
FEATURE_COLUMNS: List[str] = [
    # ── 추세 (Trend) — 7종 ──
    "ema_9",
    "ema_20",
    "ema_50",
    "ema_alignment",        # 1=정배열, -1=역배열, 0=혼조
    "macd_line",
    "macd_signal",
    "macd_histogram",
    # ── 모멘텀 (Momentum) — 3종 ──
    "rsi_14",
    "stoch_k",
    "stoch_d",
    # ── 변동성 & 거래량 (Volatility & Volume) — 4종 ──
    "bb_upper",
    "bb_lower",
    "sma_20",
    "volume_ma_ratio",
]


def compute_all_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    OHLCV DataFrame에 12종 기술적 지표 피처를 벡터 연산으로 추가합니다.

    Args:
        df: 최소 컬럼 ['open', 'high', 'low', 'close', 'volume']을 가진 DataFrame.
            시간순 정렬 (oldest → newest) 필수.

    Returns:
        피처 컬럼이 추가된 DataFrame (원본 복사본).
        Warm-up 구간의 NaN은 자연스럽게 유지됩니다.
    """
    df = df.copy()
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)

    # ════════════════════════════════════════════════════════════════════════
    # 1. 추세 (Trend)
    # ════════════════════════════════════════════════════════════════════════

    # EMA (Exponential Moving Average) — 9, 20, 50
    df["ema_9"] = close.ewm(span=9, adjust=False).mean()
    df["ema_20"] = close.ewm(span=20, adjust=False).mean()
    df["ema_50"] = close.ewm(span=50, adjust=False).mean()

    # EMA 정배열/역배열 판정 (벡터화 연산 — .apply() 미사용)
    #   1  = 정배열 (EMA9 > EMA20 > EMA50) — 강한 상승 추세
    #  -1  = 역배열 (EMA9 < EMA20 < EMA50) — 강한 하락 추세
    #   0  = 혼조 — 방향 불명확
    bull_align = (df["ema_9"] > df["ema_20"]) & (df["ema_20"] > df["ema_50"])
    bear_align = (df["ema_9"] < df["ema_20"]) & (df["ema_20"] < df["ema_50"])
    df["ema_alignment"] = np.where(
        bull_align, 1.0, np.where(bear_align, -1.0, 0.0)
    )

    # MACD (Moving Average Convergence Divergence)
    #   MACD Line   = EMA(12) - EMA(26)
    #   Signal Line = EMA(9) of MACD Line
    #   Histogram   = MACD Line - Signal Line
    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    df["macd_line"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd_line"].ewm(span=9, adjust=False).mean()
    df["macd_histogram"] = df["macd_line"] - df["macd_signal"]

    # ════════════════════════════════════════════════════════════════════════
    # 2. 모멘텀 (Momentum)
    # ════════════════════════════════════════════════════════════════════════

    # RSI (Relative Strength Index) — Wilder's Smoothing (α = 1/14)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / 14, min_periods=14, adjust=False).mean()
    # 0으로 나누기 방어: avg_loss가 0이면 RS → inf → RSI = 100 (극단적 과매수)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    # Stochastic Oscillator (%K, %D)
    #   %K = (Close - Lowest Low) / (Highest High - Lowest Low) × 100
    #   %D = SMA(3) of %K
    low_14 = low.rolling(window=14, min_periods=14).min()
    high_14 = high.rolling(window=14, min_periods=14).max()
    stoch_denom = (high_14 - low_14).replace(0, np.nan)
    df["stoch_k"] = 100.0 * (close - low_14) / stoch_denom
    df["stoch_d"] = df["stoch_k"].rolling(window=3, min_periods=3).mean()

    # ════════════════════════════════════════════════════════════════════════
    # 3. 변동성 & 거래량 (Volatility & Volume)
    # ════════════════════════════════════════════════════════════════════════

    # Bollinger Bands (SMA 20 ± 2σ)
    df["sma_20"] = close.rolling(window=20, min_periods=20).mean()
    bb_std = close.rolling(window=20, min_periods=20).std()
    df["bb_upper"] = df["sma_20"] + 2.0 * bb_std
    df["bb_lower"] = df["sma_20"] - 2.0 * bb_std

    # Volume MA Ratio (당일 거래량 / 20일 거래량 이동평균)
    #   > 1.0 → 평균 이상의 거래량 에너지 (돌파 시그널 강화)
    #   < 1.0 → 평균 이하의 거래량 (시그널 약화)
    vma_20 = volume.rolling(window=20, min_periods=20).mean().replace(0, np.nan)
    df["volume_ma_ratio"] = volume / vma_20

    logger.debug(
        "📊 [indicators] 피처 연산 완료: %d행 × %d 피처",
        len(df),
        len(FEATURE_COLUMNS),
    )
    return df


def get_feature_vector(row: pd.Series) -> np.ndarray:
    """
    DataFrame의 단일 행에서 ML 모델 입력용 1D 피처 벡터를 추출합니다.

    Args:
        row: compute_all_features()로 피처가 추가된 DataFrame의 단일 행 (pd.Series)

    Returns:
        shape=(14,) float64 numpy 배열. FEATURE_COLUMNS 순서 엄수.
    """
    return row[FEATURE_COLUMNS].values.astype(np.float64)


def has_valid_features(row: pd.Series) -> bool:
    """피처 벡터에 NaN이 없는지 (warm-up 완료 여부) 검증합니다."""
    vec = get_feature_vector(row)
    return not np.any(np.isnan(vec))
