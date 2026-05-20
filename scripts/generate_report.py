"""
scripts/generate_report.py — 수익률 분석 도구

TradeHistory 테이블의 체결 내역을 바탕으로
매수(BUY)와 매도(SELL) 쌍을 매칭하여 각 거래당 수익률(%)과 실현 손익(PnL)을 계산하고,
전체 매매 횟수, 승률(Win Rate), 누적 수익률, 최대 낙폭(MDD)을 산출합니다.
"""

import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, select

# 프로젝트 루트 경로 추가
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.config import get_settings
from app.models.models import TradeHistory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("generate_report")

def main():
    settings = get_settings()
    engine = create_engine(settings.sync_database_url)
    
    # TradeHistory 전체 데이터 로드 (FILLED or CLOSED 상태만)
    query = (
        select(TradeHistory)
        .where(TradeHistory.status.in_(["FILLED", "CLOSED"]))
        .order_by(TradeHistory.timestamp.asc())
    )
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logger.warning("📭 거래 내역이 없습니다. (TradeHistory 비어 있음)")
        return
        
    logger.info(f"✅ 총 {len(df)}건의 체결 내역을 로드했습니다.")
    
    # ── 1. BUY / SELL 매칭 및 거래 단위(Trade) 분석 ─────────────────────────
    trades = []
    current_long = None
    
    for _, row in df.iterrows():
        side = row["side"]
        price = float(row["price"])
        amount = float(row["amount"])
        ts = row["timestamp"]
        
        if side == "BUY":
            if current_long is None:
                current_long = {"entry_price": price, "entry_ts": ts, "amount": amount}
            else:
                # 이미 LONG인데 또 BUY가 온 경우 (비정상 케이스 처리)
                logger.warning(f"⚠️ 중복 BUY 주문 발견: {ts}")
                
        elif side == "SELL":
            if current_long is not None:
                # 매칭 성사 (1 Trade 완성)
                entry_price = current_long["entry_price"]
                exit_price = price
                amount = current_long["amount"]
                
                # 수익률 계산 (수수료 제외, 단순 수익률)
                # 바이낸스 현물 수수료 0.1% 가정 (왕복 0.2%)
                FEE_RATE = 0.001
                gross_pnl = (exit_price - entry_price) * amount
                entry_fee = entry_price * amount * FEE_RATE
                exit_fee = exit_price * amount * FEE_RATE
                net_pnl = gross_pnl - (entry_fee + exit_fee)
                
                return_pct = (exit_price - entry_price) / entry_price * 100
                net_return_pct = return_pct - (FEE_RATE * 2 * 100)
                
                trades.append({
                    "entry_ts": current_long["entry_ts"],
                    "exit_ts": ts,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "amount": amount,
                    "return_pct": return_pct,
                    "net_return_pct": net_return_pct,
                    "net_pnl": net_pnl,
                    "is_win": net_return_pct > 0
                })
                current_long = None
            else:
                # LONG 진입 없이 SELL 발생 (수동 매도 또는 오류)
                logger.debug(f"⚠️ 진입 없는 SELL 주문 발견: {ts}")

    if not trades:
        logger.warning("📭 매칭된 거래(BUY->SELL 쌍)가 없습니다.")
        return
        
    trade_df = pd.DataFrame(trades)
    
    # ── 2. 성과 지표 (Performance Metrics) 계산 ────────────────────────────
    total_trades = len(trade_df)
    winning_trades = len(trade_df[trade_df["is_win"] == True])
    win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
    
    total_pnl = trade_df["net_pnl"].sum()
    
    # 누적 자산 곡선 (단리 가정)
    trade_df["cum_return_pct"] = trade_df["net_return_pct"].cumsum()
    
    # MDD (Max Drawdown) 계산
    # 누적 수익률 배열에서 전고점(Peak) 대비 하락폭 계산
    trade_df["peak"] = trade_df["cum_return_pct"].cummax()
    trade_df["drawdown"] = trade_df["cum_return_pct"] - trade_df["peak"]
    mdd = trade_df["drawdown"].min()
    
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("              [ 📊 퀀트플로우 매매 성과 리포트 ]")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"🔹 총 매매 횟수  : {total_trades}회 (매칭 완료 기준)")
    logger.info(f"🔹 승률 (Win Rate): {win_rate:.2f}% ({winning_trades}/{total_trades})")
    logger.info(f"🔹 누적 수익률    : {trade_df['cum_return_pct'].iloc[-1]:.2f}% (수수료 차감 후)")
    logger.info(f"🔹 총 실현 손익   : {total_pnl:.4f} USDT")
    logger.info(f"🔹 최대 낙폭 (MDD): {mdd:.2f}%")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # 거래 내역 표(Table) 콘솔 출력
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    logger.info(f"📝 상세 거래 내역:\n{trade_df[['entry_ts', 'exit_ts', 'entry_price', 'exit_price', 'net_return_pct', 'net_pnl', 'cum_return_pct']]}")

if __name__ == "__main__":
    main()
