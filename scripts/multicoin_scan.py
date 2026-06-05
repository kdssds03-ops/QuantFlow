"""여러 코인에서 4h 추세추종 엣지(Sharpe) 비교 — '다른 코인이 더 나은가' 실증.

4h 캔들을 직접 수집(1m 대비 240배 적음)하여 다수 코인을 빠르게 비교.
같은 전략(EMA 30/60 교차, 수수료 0.05% 편도)을 모든 코인에 적용.
"""
import sys, time
from pathlib import Path
import numpy as np
import pandas as pd
import ccxt

sys.path.append(str(Path(__file__).resolve().parent.parent))

FEE = 0.0005
BARS_PER_YEAR_4H = 365 * 6  # 4h봉/년

COINS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "XRP/USDT",
         "AVAX/USDT", "LINK/USDT", "BNB/USDT", "1000PEPE/USDT", "1000SHIB/USDT"]


def fetch_4h(ex, symbol, days=1095):
    end = ex.milliseconds()
    since = end - days * 86400 * 1000
    rows, cur = [], since
    while cur < end:
        b = ex.fetch_ohlcv(symbol, "4h", since=cur, limit=1500)
        if not b:
            break
        rows.extend(b)
        nxt = b[-1][0] + 4 * 3600 * 1000
        if nxt <= cur:        # 타임스탬프가 더 진행 안 하면 종료 (무한루프 방지)
            break
        cur = nxt
        time.sleep(0.05)
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df.drop_duplicates("ts", inplace=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df.reset_index(drop=True)


def ema_cross(d, fast=30, slow=60):
    ef = d["close"].ewm(span=fast, adjust=False).mean()
    es = d["close"].ewm(span=slow, adjust=False).mean()
    return np.sign(ef - es)


def bt(d, pos, fee=FEE):
    ret = d["close"].pct_change().fillna(0.0)
    pos = pos.shift(1).fillna(0.0)
    turn = pos.diff().abs().fillna(pos.abs())
    strat = pos * ret - turn * fee
    eq = (1 + strat).cumprod()
    total = eq.iloc[-1] - 1
    sharpe = strat.mean() / strat.std() * np.sqrt(BARS_PER_YEAR_4H) if strat.std() > 0 else 0
    mdd = (eq / eq.cummax() - 1).min()
    # 롱/숏 분리
    lp = strat[pos > 0].sum() * 100
    sp = strat[pos < 0].sum() * 100
    vol = ret.std() * np.sqrt(BARS_PER_YEAR_4H) * 100  # 연율 변동성
    return total * 100, sharpe, mdd * 100, lp, sp, vol


def main():
    ex = ccxt.binanceusdm({"enableRateLimit": True})
    print(f"{'코인':<14} {'기간BH':>8} {'변동성':>7} | {'추세 3년':>9} {'CAGR':>7} {'Sharpe':>7} {'MDD':>6} | {'롱':>6} {'숏':>6}  엣지")
    print("-" * 100)
    results = []
    for sym in COINS:
        try:
            d = fetch_4h(ex, sym)
            if len(d) < 200:
                print(f"{sym:<14} 데이터부족({len(d)})")
                continue
            yrs = (d["dt"].iloc[-1] - d["dt"].iloc[0]).days / 365
            bh = (d["close"].iloc[-1] / d["close"].iloc[0] - 1) * 100
            tot, sh, mdd, lp, sp, vol = bt(d, ema_cross(d))
            cagr = ((1 + tot / 100) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
            edge = "★강함" if (sh > 0.8 and lp > 0 and sp > 0) else (
                   "양호" if (sh > 0.4 and lp > 0 and sp > 0) else "약함")
            print(f"{sym:<14} {bh:+7.0f}% {vol:6.0f}% | {tot:+8.0f}% {cagr:+6.1f}% {sh:+6.2f} {mdd:5.0f}% | "
                  f"{lp:+5.0f}% {sp:+5.0f}%  {edge}")
            results.append((sym, sh, cagr, mdd, lp, sp))
        except Exception as e:
            print(f"{sym:<14} 오류: {str(e)[:50]}")
    print("-" * 100)
    if results:
        top = sorted(results, key=lambda x: x[1], reverse=True)
        print("\n[Sharpe 순위 — 엣지가 강한 코인]")
        for sym, sh, cagr, mdd, lp, sp in top:
            print(f"  {sym:<14} Sharpe {sh:+.2f}  CAGR {cagr:+.1f}%  MDD {mdd:.0f}%  (롱 {lp:+.0f}% / 숏 {sp:+.0f}%)")


if __name__ == "__main__":
    main()
