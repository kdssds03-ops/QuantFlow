"""
scripts/train_model.py — ML 예측 모델 학습 스크립트

실행 방법 (프로젝트 루트에서):
    python -m scripts.train_model

기능:
  1. PostgreSQL market_data 테이블에서 전체 이력 데이터 로드
  2. 피처 엔지니어링 (OHLCV + 기술 지표 + 파생 변수)
  3. 3진 분류 Target 생성 (UP=2, FLAT=1, DOWN=0)
  4. TimeSeriesSplit 교차 검증으로 LightGBM 학습
  5. 학습된 모델을 worker/model.pkl 로 저장
"""

import logging
import math
import os
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_ta as ta
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import lightgbm as lgb
from sqlalchemy import create_engine, text

# ── 프로젝트 루트를 sys.path에 추가 (모듈 임포트 경로 설정) ──────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.config import get_settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_model")

# ── 상수 ─────────────────────────────────────────────────────────────────
MODEL_SAVE_PATH = PROJECT_ROOT / "worker" / "model.pkl"
UP_THRESHOLD   = 0.0015   # +0.15% 이상 → UP (Class 2 = BUY)
DOWN_THRESHOLD = -0.0015  # -0.15% 이하 → DOWN (Class 0 = SELL)


# ─────────────────────────────────────────────────────────────────────────
# 1. 데이터 로드
# ─────────────────────────────────────────────────────────────────────────
def load_market_data(engine) -> pd.DataFrame:
    """PostgreSQL market_data 테이블 전체를 로드하여 DataFrame 반환."""
    query = text("""
        SELECT
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            sma_20,
            rsi_14,
            bb_upper,
            bb_lower
        FROM market_data
        ORDER BY timestamp ASC
    """)
    df = pd.read_sql(query, engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.set_index("timestamp", inplace=True)

    # Decimal → float 변환
    numeric_cols = ["open", "high", "low", "close", "volume",
                    "sma_20", "rsi_14", "bb_upper", "bb_lower"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"✅ 데이터 로드 완료: {len(df)}행 × {df.shape[1]}열")
    return df


# ─────────────────────────────────────────────────────────────────────────
# 2. 피처 엔지니어링
# ─────────────────────────────────────────────────────────────────────────
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    원시 OHLCV + 기존 지표에 파생 변수를 추가하여 Feature DataFrame 반환.

    추가 파생 변수:
        - return_1   : 1-캔들 수익률 (Close-to-Close)
        - return_3   : 3-캔들 수익률
        - return_5   : 5-캔들 수익률
        - volatility_5  : 최근 5캔들 std(수익률) — 단기 변동성
        - volatility_20 : 최근 20캔들 std(수익률) — 중기 변동성
        - sma_dist      : (Close - SMA20) / SMA20 × 100 — 이격도(%)
        - bb_width      : (BB_upper - BB_lower) / SMA20 × 100 — 밴드폭(%)
        - bb_pct        : (Close - BB_lower) / (BB_upper - BB_lower) — 위치 비율
        - rsi_lag1      : RSI 1-캔들 전 값 (모멘텀 변화 포착)
        - rsi_diff      : RSI 변화량 (RSI - RSI_lag1)
        - volume_ratio  : 현재 거래량 / 20-캔들 평균 거래량
        - high_low_range: (High - Low) / Close × 100 — 당일 가격 범위(%)
        - upper_shadow  : (High - max(Open,Close)) / (High-Low) — 윗꼬리 비율
        - lower_shadow  : (min(Open,Close) - Low)  / (High-Low) — 아랫꼬리 비율
        - close_open_pct: (Close - Open) / Open × 100 — 봉 자체 수익률(%)
    """
    df = df.copy()

    # ── 기술 지표 추가 (MACD, ATR) ────────────────────────────────────
    # 실시간 예측 시에도 pandas-ta로 쉽게 계산하기 위해 추가
    df.ta.macd(append=True)
    df.ta.atr(append=True)

    # ── 기본 수익률 ───────────────────────────────────────────────────
    df["return_1"] = df["close"].pct_change(1)
    df["return_3"] = df["close"].pct_change(3)
    df["return_5"] = df["close"].pct_change(5)

    # ── 변동성 ───────────────────────────────────────────────────────
    df["volatility_5"]  = df["return_1"].rolling(5).std()
    df["volatility_20"] = df["return_1"].rolling(20).std()

    # ── 이동평균 이격도 ───────────────────────────────────────────────
    df["sma_dist"] = (df["close"] - df["sma_20"]) / df["sma_20"] * 100

    # ── 볼린저밴드 파생 ───────────────────────────────────────────────
    bb_range = df["bb_upper"] - df["bb_lower"]
    df["bb_width"] = bb_range / df["sma_20"] * 100
    # 분모가 0이 되는 케이스 방지
    df["bb_pct"]   = (df["close"] - df["bb_lower"]) / bb_range.replace(0, np.nan)

    # ── RSI 파생 ─────────────────────────────────────────────────────
    df["rsi_lag1"] = df["rsi_14"].shift(1)
    df["rsi_diff"] = df["rsi_14"] - df["rsi_lag1"]

    # ── 거래량 비율 ───────────────────────────────────────────────────
    df["volume_ratio"] = df["volume"] / df["volume"].rolling(20).mean()

    # ── 캔들 형태 ─────────────────────────────────────────────────────
    hl_range = df["high"] - df["low"]
    safe_hl   = hl_range.replace(0, np.nan)
    df["high_low_range"]  = hl_range / df["close"] * 100
    df["upper_shadow"]    = (df["high"] - df[["open", "close"]].max(axis=1)) / safe_hl
    df["lower_shadow"]    = (df[["open", "close"]].min(axis=1) - df["low"]) / safe_hl
    df["close_open_pct"]  = (df["close"] - df["open"]) / df["open"] * 100

    logger.info(f"✅ 피처 엔지니어링 완료: {df.shape[1]}열")
    return df


# ─────────────────────────────────────────────────────────────────────────
# 3. Target(Y값) 생성
# ─────────────────────────────────────────────────────────────────────────
def build_target(df: pd.DataFrame) -> pd.Series:
    """
    5분 뒤 종가 기준 3진 분류 레이블 생성.
      - 5분 뒤 수익률 >= +0.08% → 2 (UP / BUY)
      - 5분 뒤 수익률 <= -0.08% → 0 (DOWN / SELL)
      - 그 외                   → 1 (FLAT / HOLD)
    """
    # 5분 뒤 종가를 현재 종가로 나눈 수익률
    next_5m_return = (df["close"].shift(-5) - df["close"]) / df["close"]

    conditions = [
        next_5m_return >= UP_THRESHOLD,
        next_5m_return <= DOWN_THRESHOLD,
    ]
    choices = [2, 0]  # UP=2, DOWN=0
    target = pd.Series(
        np.select(conditions, choices, default=1),
        index=df.index,
        name="target",
    )
    
    # 마지막 5개 캔들은 shift(-5)로 인해 미래 데이터를 알 수 없으므로 NaN 처리
    if len(target) >= 5:
        target.iloc[-5:] = np.nan

    dist = target.value_counts().sort_index()
    logger.info(
        f"✅ 타겟 분포: DOWN(0)={dist.get(0, 0)}, "
        f"FLAT(1)={dist.get(1, 0)}, UP(2)={dist.get(2, 0)}"
    )
    return target


# ─────────────────────────────────────────────────────────────────────────
# 4. 모델 학습
# ─────────────────────────────────────────────────────────────────────────
FEATURE_COLS = [
    # 원본 OHLCV
    "open", "high", "low", "close", "volume",
    # 기존 기술 지표
    "sma_20", "rsi_14", "bb_upper", "bb_lower",
    # 파생 변수
    "return_1", "return_3", "return_5",
    "volatility_5", "volatility_20",
    "sma_dist",
    "bb_width", "bb_pct",
    "rsi_lag1", "rsi_diff",
    "volume_ratio",
    "high_low_range", "upper_shadow", "lower_shadow", "close_open_pct",
    # ── MACD & ATR 파생 피처 ──────────────────────────────────────────
    "MACD_12_26_9", "MACDh_12_26_9", "MACDs_12_26_9", "ATRr_14",
]


def train(df_feat: pd.DataFrame, target: pd.Series) -> lgb.LGBMClassifier:
    """
    시계열 80:20 (Train:Test) Split으로 LightGBM 학습.
    """
    # 유효 피처만 선택
    available_features = [c for c in FEATURE_COLS if c in df_feat.columns]
    X_all = df_feat[available_features]
    y_all = target

    # ── NaN 행 제거 (warm-up 기간 및 마지막 5캔들) ─────────────────────────
    valid_mask = X_all.notna().all(axis=1) & y_all.notna()
    X_all = X_all[valid_mask]
    y_all = y_all[valid_mask]
    logger.info(f"✅ 유효 샘플: {len(X_all)}행 (NaN 제거 후)")

    if len(X_all) < 200:
        raise ValueError("학습 데이터 부족: 최소 200행 이상 필요.")

    # ── 80% Train / 20% Test 분리 (Shuffle=False) ─────────────────────────
    split_idx = int(len(X_all) * 0.8)
    X_train, X_test = X_all.iloc[:split_idx], X_all.iloc[split_idx:]
    y_train, y_test = y_all.iloc[:split_idx], y_all.iloc[split_idx:]
    logger.info(f"✅ 시계열 Split: Train {len(X_train)}행, Test {len(X_test)}행")

    # ── LightGBM 파라미터 ─────────────────────────────────────────────────
    lgbm_params = dict(
        objective="multiclass",
        num_class=3,
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=20,
        colsample_bytree=0.8,
        subsample=0.8,
        subsample_freq=1,
        reg_alpha=0.1,
        reg_lambda=0.1,
        class_weight="balanced",   # 클래스 불균형 해소
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )

    # ── 학습 및 평가 ───────────────────────────────────────────────────────
    logger.info("🔄 LightGBM 학습 시작...")
    model = lgb.LGBMClassifier(**lgbm_params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False),
                   lgb.log_evaluation(period=50)],
    )

    y_pred = model.predict(X_test)

    logger.info("\n📊 [Test Set Evaluation - Classification Report]")
    logger.info("\n" + classification_report(
        y_test, y_pred,
        target_names=["DOWN(0)", "FLAT(1)", "UP(2)"],
    ))

    logger.info("📊 [Confusion Matrix]")
    cm = confusion_matrix(y_test, y_pred)
    logger.info(f"\n{cm}")

    # 피처 중요도 Top-10
    importances = pd.Series(
        model.feature_importances_,
        index=available_features,
    ).sort_values(ascending=False)
    logger.info(f"\n🏆 피처 중요도 Top-10:\n{importances.head(10).to_string()}")

    return model, available_features


# ─────────────────────────────────────────────────────────────────────────
# 5. 모델 저장
# ─────────────────────────────────────────────────────────────────────────
def save_model(model, feature_cols: list, path: Path):
    """
    모델과 사용된 피처 컬럼 목록을 함께 pickle로 저장.
    MLPredictor가 로드 시 동일한 컬럼 순서를 보장하기 위해 함께 저장.
    """
    payload = {
        "model": model,
        "feature_cols": feature_cols,
        "up_threshold":   UP_THRESHOLD,
        "down_threshold": DOWN_THRESHOLD,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(payload, f)
    size_kb = path.stat().st_size / 1024
    logger.info(f"💾 모델 저장 완료: {path}  ({size_kb:.1f} KB)")


# ─────────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────────
def main():
    settings = get_settings()
    engine = create_engine(
        settings.sync_database_url,
        pool_pre_ping=True,
    )

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  QuantFlow ML 모델 학습 시작 (LightGBM 3-Class)")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # 1. 데이터 로드
    df_raw = load_market_data(engine)

    # 2. 피처 엔지니어링
    df_feat = build_features(df_raw)

    # 3. 타겟 생성
    target = build_target(df_feat)

    # 4. 학습
    model, feature_cols = train(df_feat, target)

    # 5. 저장
    save_model(model, feature_cols, MODEL_SAVE_PATH)

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  ✅ 학습 완료!")
    logger.info(f"  모델 경로: {MODEL_SAVE_PATH}")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    main()
