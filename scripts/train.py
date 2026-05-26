import sys
import os
import logging
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score

# 프로젝트 루트 경로를 sys.path에 추가하여 내부 모듈 임포트 가능하게 설정
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# ta 라이브러리 (Python 3.12 공식 지원)
import ta

# 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def train_model():
    logger.info("🚀 [QuantFlow ML Pipeline] XGBoost 오프라인 학습 시작")
    
    # 1. 로컬 CSV 파일에서 시계열 데이터 전량 로드 (과거 -> 최신)
    logger.info("1️⃣ 로컬 CSV 파일에서 시계열 데이터 로드 중...")
    csv_path = os.path.join(project_root, "btc_1m_1year.csv")
    if not os.path.exists(csv_path):
        logger.error(f"❌ 로컬 CSV 파일이 없습니다: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # 시간 순서 정렬 (timestamp 기준)
    if 'timestamp_ms' in df.columns:
        df = df.sort_values("timestamp_ms").reset_index(drop=True)
    elif 'timestamp' in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)

    if df.empty:
        logger.error("❌ CSV 파일에 데이터가 없습니다.")
        return

    logger.info(f"✅ 총 {len(df):,}개의 캔들 데이터 로드 완료")
    
    # 2. 피처 엔지니어링 (pandas-ta 활용)
    logger.info("2️⃣ 보조지표(SMA, RSI, Bollinger Bands) 계산 중...")
    # 부동소수점 오차 방지를 위해 float 타입으로 안전하게 변환
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
        
    # ta 패키지를 활용한 1기 기본 지표 계산 (SMA, RSI, Bollinger Bands)
    df['sma_20'] = ta.trend.sma_indicator(close=df['close'], window=20)
    df['rsi_14'] = ta.momentum.rsi(close=df['close'], window=14)
    
    indicator_bb = ta.volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb_upper'] = indicator_bb.bollinger_hband()
    df['bb_lower'] = indicator_bb.bollinger_lband()
    
    # 신규 2기 3대 고급 피처 주입
    df['atr_14'] = ta.volatility.average_true_range(high=df['high'], low=df['low'], close=df['close'], window=14)
    df['macd_line'] = ta.trend.macd(close=df['close'], window_fast=12, window_slow=26)
    df['macd_signal'] = ta.trend.macd_signal(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
    
    # 모델 학습에 사용할 최종 11개 피처
    feature_cols = ['open', 'high', 'low', 'close', 'volume', 'sma_20', 'rsi_14', 'bb_upper', 'atr_14', 'macd_line', 'macd_signal']
    
    # 3. 결측치 제거
    # 지표 계산 초기 구간(최대 20개 캔들)은 NaN이 발생하므로 깔끔하게 제거
    df.dropna(subset=feature_cols, inplace=True)
    
    # 4. 미래 예측 라벨링 (Target Labeling)
    # 각 캔들 시점에서 '다음 1분 뒤의 종가'가 '현재 종가'보다 높으면 1 (상승), 낮거나 같으면 0 (하락)
    df['next_close'] = df['close'].shift(-1)
    df['target'] = (df['next_close'] > df['close']).astype(int)
    
    # 가장 마지막 행은 미래를 알 수 없어 next_close가 NaN이므로 제거
    df.dropna(subset=['next_close'], inplace=True)
    
    # 5. 시계열 윈도우 변환 및 평탄화 (Sliding Window & Flattening)
    logger.info("3️⃣ 60윈도우 기반 학습용 매트릭스 변환 중... (이 작업은 다소 시간이 걸릴 수 있습니다.)")
    window_size = 60
    
    # 넘파이 배열로 변환하여 슬라이싱 고속화
    feature_values = df[feature_cols].values
    target_values = df['target'].values
    
    X, y = [], []
    
    # 과거 60개 캔들을 하나의 행(Row)으로 묶어 660차원 벡터로 변환
    for i in range(len(df) - window_size):
        window = feature_values[i:i + window_size]
        X.append(window.flatten())
        y.append(target_values[i + window_size - 1])
        
    X = np.array(X, dtype=np.float32)
    y = np.array(y, dtype=int)
    
    logger.info(f"✅ X 데이터셋 Shape: {X.shape}, y 타겟 Shape: {y.shape}")
    
    # 6. 데이터 분할 (Time-series Split: 80% / 20%)
    # 시계열 순서가 섞이지 않게 분할(Shuffle = False)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    logger.info(f"📊 Train Set: {X_train.shape[0]:,}개 | Validation Set: {X_test.shape[0]:,}개")
    
    # 7. XGBoost 모델 학습
    logger.info("4️⃣ XGBoost 모델 학습 진행 중...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.05,
        random_state=42,
        eval_metric='logloss'
    )
    
    # 검증 셋을 인자로 넘겨 학습 추이를 모니터링
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=10)
    
    # 8. 검증 셋 정확도 평가
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"🏆 학습 완료! Validation Accuracy: {accuracy * 100:.2f}%")
    
    # 모델 파일 영구 저장
    save_dir = os.path.join(project_root, "data")
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, "xgb_btc_v2.json")
    
    model.save_model(save_path)
    logger.info(f"💾 모델 가중치 영구 저장 완료: {save_path}")

if __name__ == "__main__":
    train_model()
