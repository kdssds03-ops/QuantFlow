import os
import sys
import time
import logging
import datetime
from pathlib import Path
from decimal import Decimal

import pandas as pd
import numpy as np
import ccxt

# 환경변수 / 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("backtest")

# 루트 디렉토리를 sys.path에 추가 (worker.indicators 임포트용)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

try:
    from worker.indicators import compute_all_features
except ImportError:
    logger.error("worker.indicators 모듈을 찾을 수 없습니다. 경로 설정을 확인하세요.")
    sys.exit(1)


class BacktestConfig:
    SYMBOL = 'BTC/USDT'
    TIMEFRAME = '1m'
    DATA_FILE = PROJECT_ROOT / 'data' / 'btc_1m_1year.csv'
    
    INITIAL_BALANCE = 10000.0  # 초기 자본금 (USDT)
    ENTRY_PORTION = 0.10       # 1회 진입 시 가용 자본금 비율 (10%)
    FEE_RATE = 0.0005          # 바이낸스 시장가 수수료 기준 (0.05%)
    
    # 숏(Short) 포지션 리스크 관리
    TP_PCT = 0.030             # 하드 익절 (진입가 대비 3.0% 하락 시)
    SL_PCT = 0.015             # 하드 손절 (진입가 대비 1.5% 상승 시)
    TIMEOUT_MINS = 180         # 최대 보유 시간 (180분)


def fetch_binance_data(symbol: str, timeframe: str, days: int = 365) -> pd.DataFrame:
    """바이낸스 API를 통해 1년치 1분봉 데이터를 분할 수집하여 반환합니다."""
    exchange = ccxt.binance({'enableRateLimit': True})
    
    # 목표 종료 시간 (현재) 및 시작 시간 (1년 전)
    end_time = exchange.milliseconds()
    start_time = end_time - (days * 24 * 60 * 60 * 1000)
    
    all_ohlcv = []
    current_start = start_time
    
    logger.info(f"데이터 수집 시작: {symbol} {timeframe}, {days}일치...")
    
    while current_start < end_time:
        try:
            # 바이낸스 최대 1000개 요청
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=current_start, limit=1000)
            
            if not ohlcv:
                break
                
            all_ohlcv.extend(ohlcv)
            
            # 다음 요청의 시작 시간은 마지막 수집된 캔들의 타임스탬프 + 1분(60000ms)
            last_ts = ohlcv[-1][0]
            current_start = last_ts + 60000
            
            # 진행률 로깅 (약 525번의 요청 중 일부만)
            if len(all_ohlcv) % 50000 == 0:
                logger.info(f"데이터 수집 중... {len(all_ohlcv)} 캔들 수집 완료")
                
            time.sleep(0.1) # 안전한 Rate Limit 준수
            
        except Exception as e:
            logger.error(f"데이터 수집 중 오류 발생: {e}")
            time.sleep(2.0)
            
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # 중복 제거 및 시간순 정렬
    df.drop_duplicates(subset=['timestamp'], inplace=True)
    df.sort_values(by='timestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    logger.info(f"총 {len(df)} 캔들 수집 완료.")
    return df


def get_data() -> pd.DataFrame:
    """로컬 CSV가 있으면 로드하고, 없으면 수집하여 저장합니다."""
    # data 폴더가 없으면 생성
    BacktestConfig.DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    if BacktestConfig.DATA_FILE.exists():
        logger.info(f"로컬 캐시 파일 발견: {BacktestConfig.DATA_FILE}")
        df = pd.read_csv(BacktestConfig.DATA_FILE)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    else:
        logger.info("로컬 캐시 파일이 없습니다. API에서 데이터를 수집합니다.")
        df = fetch_binance_data(BacktestConfig.SYMBOL, BacktestConfig.TIMEFRAME, days=365)
        df.to_csv(BacktestConfig.DATA_FILE, index=False)
        logger.info(f"데이터 캐싱 완료: {BacktestConfig.DATA_FILE}")
        return df


def run_backtest(df: pd.DataFrame):
    """지표가 포함된 DataFrame을 순회하며 가상 매매를 수행합니다."""
    balance = BacktestConfig.INITIAL_BALANCE
    position = 0 # 0: None, -1: Short
    
    entry_price = 0.0
    entry_idx = 0
    qty = 0.0
    
    trades = []
    balance_history = []
    
    logger.info("가상 매매 시뮬레이션을 시작합니다...")
    
    # Numpy 배열로 추출하여 순회 (속도 최적화)
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    
    bb_uppers = df['bb_upper'].values
    bb_lowers = df['bb_lower'].values
    sma_20s = df['sma_20'].values
    rsi_14s = df['rsi_14'].values
    datetimes = df['datetime'].values
    
    for i in range(len(df)):
        # Warm-up 체크 (지표가 NaN이면 스킵)
        if np.isnan(bb_uppers[i]) or np.isnan(rsi_14s[i]):
            balance_history.append(balance)
            continue
            
        current_close = closes[i]
        current_rsi = rsi_14s[i]
        current_bb_upper = bb_uppers[i]
        current_bb_lower = bb_lowers[i]
        current_sma_20 = sma_20s[i]
        current_time = datetimes[i]
        
        # --- 포지션 청산 로직 ---
        if position == -1:
            duration_mins = i - entry_idx
            
            # 1. 하드 익절 / 하드 손절 체크 (고점/저점 비교로 정확도 향상 가능하나 여기선 Close 종가로 단순화)
            # Short 익절: 가격이 진입가 대비 SL_PCT 만큼 하락
            tp_price = entry_price * (1.0 - BacktestConfig.TP_PCT)
            sl_price = entry_price * (1.0 + BacktestConfig.SL_PCT)
            
            exit_reason = None
            
            if current_close <= tp_price:
                exit_reason = "Hard TP"
            elif current_close >= sl_price:
                exit_reason = "Hard SL"
            elif duration_mins >= BacktestConfig.TIMEOUT_MINS:
                exit_reason = "Timeout"
            elif current_close < current_sma_20:
                exit_reason = "Signal Exit (SMA20 Break)"
            elif current_rsi <= 40:
                exit_reason = "Signal Exit (RSI <= 40)"
                
            if exit_reason:
                # 청산 연산
                exit_value = qty * current_close
                fee_exit = exit_value * BacktestConfig.FEE_RATE
                
                # Short PnL = (진입가 - 청산가) * 수량
                pnl = (entry_price - current_close) * qty
                
                balance += pnl - fee_exit
                
                trades.append({
                    'entry_time': datetimes[entry_idx],
                    'exit_time': current_time,
                    'entry_price': entry_price,
                    'exit_price': current_close,
                    'duration': duration_mins,
                    'pnl': pnl - fee_exit,
                    'reason': exit_reason
                })
                
                position = 0
                entry_price = 0.0
                qty = 0.0
                entry_idx = 0
        
        # --- 포지션 진입 로직 ---
        # 청산된 당일(봉)에는 재진입하지 않음(단순화)
        if position == 0:
            if current_close > current_bb_upper and current_rsi >= 70:
                # Short 진입
                position = -1
                entry_price = current_close
                entry_idx = i
                
                # 자금 10% 투입
                invest_usdt = balance * BacktestConfig.ENTRY_PORTION
                qty = invest_usdt / entry_price
                
                fee_entry = invest_usdt * BacktestConfig.FEE_RATE
                balance -= fee_entry
                
        balance_history.append(balance)
        
    return trades, balance_history


def calculate_mdd(balance_history: list) -> float:
    """최대 낙폭(MDD) 계산"""
    balances = np.array(balance_history)
    peak = np.maximum.accumulate(balances)
    drawdown = (balances - peak) / peak
    mdd = drawdown.min()
    return mdd


def main():
    logger.info("=" * 50)
    logger.info("QuantFlow v9.0.0 Backtesting Engine")
    logger.info("=" * 50)
    
    # 1. 데이터 준비
    df = get_data()
    
    # 2. 지표 연산
    logger.info("기술적 지표(Feature)를 계산합니다...")
    df = compute_all_features(df)
    
    # 3. 백테스트 실행
    trades, balance_history = run_backtest(df)
    
    # 4. 성과 분석
    final_balance = balance_history[-1]
    total_return_pct = (final_balance / BacktestConfig.INITIAL_BALANCE - 1) * 100
    
    total_trades = len(trades)
    winning_trades = [t for t in trades if t['pnl'] > 0]
    win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0
    
    mdd = calculate_mdd(balance_history) * 100
    
    # 5. 결과 출력
    print("\n" + "=" * 50)
    print("📈 백테스트 결과 리포트")
    print("=" * 50)
    print(f"💰 초기 자산    : {BacktestConfig.INITIAL_BALANCE:,.2f} USDT")
    print(f"💎 최종 자산    : {final_balance:,.2f} USDT")
    print(f"🚀 총 수익률    : {total_return_pct:+.2f} %")
    print(f"📉 최대 낙폭(MDD): {mdd:.2f} %")
    print("-" * 50)
    print(f"📊 총 거래 횟수 : {total_trades} 회")
    print(f"🏆 승률         : {win_rate:.2f} %")
    print("=" * 50)
    
    if trades:
        print("\n최근 5건의 거래 내역:")
        for t in trades[-5:]:
            print(f"- {pd.to_datetime(t['entry_time']).strftime('%Y-%m-%d %H:%M')} 진입 -> "
                  f"{pd.to_datetime(t['exit_time']).strftime('%m-%d %H:%M')} 청산 "
                  f"| 손익: {t['pnl']:+.2f} USDT | 사유: {t['reason']}")


if __name__ == "__main__":
    main()
