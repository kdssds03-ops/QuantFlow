"""
scripts/regime_validation.py — 국면별 기대수익 분포 검증 (다년 데이터)

목적: '단일 6개월 하락장' 한계를 넘어, 다년 데이터를 월별로 쪼개
각 월을 상승/하락/횡보 국면으로 라벨링하고, 4h 추세추종(TREND)과
현행 평균회귀(MeanRev), 벤치마크(매수보유)의 월수익 분포를 국면별로 집계.
→ '예상 수익률'을 단일 숫자가 아닌 '국면별 분포'로 정직하게 제시.

수수료 0.05%(편도) 반영. 추세 신호는 4h EMA(30/60) 교차(검증된 설정).
주의: 벡터화 = 풀투자(100%) 기준. 라이브 10% 사이징이면 수익·MDD가 ~1/10로 비례 축소.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
from scripts.signal_research import (  # noqa: E402
    load_1m, resample, backtest_vec, sig_ema_cross, sig_mr_bb_rsi, FEE, BARS_PER_YEAR_1M,
)

# 월 국면 라벨 임계치 (해당 월 BTC 수익률 기준)
BULL = 0.08    # +8% 이상 → 상승장
BEAR = -0.08   # -8% 이하 → 하락장


def monthly_returns(d, pos, fee=FEE):
    ret = d["close"].pct_change().fillna(0.0)
    ps = pos.shift(1).fillna(0.0)
    turn = ps.diff().abs().fillna(ps.abs())
    pnl = ps * ret - turn * fee
    s = pd.DataFrame({"dt": d["dt"], "pnl": pnl, "ret": ret}).set_index("dt")
    strat_m = (s["pnl"].resample("ME").sum() * 100)
    bh_m = (s["ret"].resample("ME").sum() * 100)  # 근사 (월내 합)
    return strat_m, bh_m


def regime_of(bh_pct):
    if bh_pct >= BULL * 100:
        return "상승장"
    if bh_pct <= BEAR * 100:
        return "하락장"
    return "횡보장"


def summarize(label, monthly, bh_monthly):
    rows = []
    for m, v in monthly.items():
        reg = regime_of(bh_monthly.get(m, 0.0))
        rows.append((m, reg, v))
    df = pd.DataFrame(rows, columns=["month", "regime", "ret"])
    win_pct = (df["ret"] > 0).mean() * 100
    print(f"\n[{label}] 월수익 분포 (풀투자 기준)")
    print(f"  전체 {len(df)}개월: 평균 {df['ret'].mean():+.2f}%/월  "
          f"중앙값 {df['ret'].median():+.2f}%  흑자월 {win_pct:.0f}%")
    for reg in ("상승장", "하락장", "횡보장"):
        sub = df[df["regime"] == reg]
        if len(sub) == 0:
            continue
        print(f"    - {reg} {len(sub):>2}개월: 평균 {sub['ret'].mean():+6.2f}%/월  "
              f"중앙값 {sub['ret'].median():+6.2f}%  흑자월 {(sub['ret']>0).mean()*100:.0f}%  "
              f"최악 {sub['ret'].min():+.1f}%  최고 {sub['ret'].max():+.1f}%")
    return df


def main():
    raw = load_1m()
    span = (raw["dt"].iloc[-1] - raw["dt"].iloc[0]).days
    bh_total = raw["close"].iloc[-1] / raw["close"].iloc[0] - 1
    print(f"[데이터] {len(raw):,}봉, {span}일 "
          f"({raw['dt'].iloc[0].date()}~{raw['dt'].iloc[-1].date()}) | "
          f"기간 BTC 매수보유 {bh_total*100:+.1f}%")

    d4 = resample(raw, "4h")
    # TREND: 4h EMA교차 30/60
    trend_m, bh4_m = monthly_returns(d4, sig_ema_cross(d4, 30, 60))
    # MeanRev: 현행 계열 (5m에서, 거래빈도 현실화)
    d5 = resample(raw, "5min")
    mr_m, bh5_m = monthly_returns(d5, sig_mr_bb_rsi(d5))

    t = summarize("TREND 4h추세추종", trend_m, bh4_m)
    summarize("현행 MeanRev 평균회귀(5m)", mr_m, bh5_m)

    # 국면 발생 빈도
    regs = [regime_of(v) for v in bh4_m]
    from collections import Counter
    c = Counter(regs)
    print(f"\n[국면 발생빈도] 총 {len(regs)}개월 중 "
          + ", ".join(f"{k} {v}개월({v/len(regs)*100:.0f}%)" for k, v in c.most_common()))
    print("\n※ 풀투자 기준. 라이브 10% 사이징이면 위 월수익·MDD를 ~1/10로 보세요.")


if __name__ == "__main__":
    main()
