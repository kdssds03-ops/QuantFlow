"""
scripts/signal_research.py — 엣지 탐색 (과최적화 방지: 학습/검증 분리)

목적: 현재 BB+RSI 평균회귀 신호가 엣지 0임이 드러났으므로,
여러 신호 계열 × 여러 타임프레임을 벡터화 백테스트로 스캔하여
'학습구간(앞 70%)뿐 아니라 검증구간(뒤 30%)에서도' 양(+)의 위험조정수익을
내는 신호만 추린다. 과최적화된 신호는 검증구간에서 무너지므로 걸러진다.

벤치마크(매수보유/매도보유)를 함께 출력하여 '추세 무임승차'를 식별한다.
수수료: 포지션 변경분에만 fee_rate를 부과 (왕복 0.1% taker 기본).
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

CSV = PROJECT_ROOT / "data" / "btc_1m_1year.csv"
FEE = 0.0005          # 편도 수수료 (taker 0.05%)
BARS_PER_YEAR_1M = 525_600


def load_1m():
    df = pd.read_csv(CSV)
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df[["dt", "open", "high", "low", "close", "volume"]].copy()


def resample(df, rule):
    g = df.set_index("dt").resample(rule)
    out = g.agg({"open": "first", "high": "max", "low": "min",
                 "close": "last", "volume": "sum"}).dropna().reset_index()
    return out


# ── 신호 생성기들: 각자 position 시리즈(+1 롱 / -1 숏 / 0 관망) 반환 ──────────
def sig_buyhold(d):
    return pd.Series(1.0, index=d.index)

def sig_shorthold(d):
    return pd.Series(-1.0, index=d.index)

def sig_mr_bb_rsi(d, n=20, k=2.0, lo=30, hi=70):
    """평균회귀(현행 계열): 하단+과매도→롱, 상단+과매수→숏."""
    c = d["close"]
    sma = c.rolling(n).mean(); sd = c.rolling(n).std()
    rsi = _rsi(c, 14)
    pos = pd.Series(0.0, index=d.index)
    pos[(c <= sma - k*sd) & (rsi <= lo)] = 1.0
    pos[(c >= sma + k*sd) & (rsi >= hi)] = -1.0
    return pos.replace(0.0, np.nan).ffill().fillna(0.0)

def sig_ema_cross(d, fast=20, slow=60):
    """추세추종: 단기EMA>장기EMA → 롱, 아니면 숏."""
    ef = d["close"].ewm(span=fast, adjust=False).mean()
    es = d["close"].ewm(span=slow, adjust=False).mean()
    return np.sign(ef - es)

def sig_donchian(d, n=60):
    """돈치안 채널 돌파(모멘텀): n봉 신고가 돌파→롱, 신저가 이탈→숏, 유지."""
    hh = d["high"].rolling(n).max().shift(1)
    ll = d["low"].rolling(n).min().shift(1)
    pos = pd.Series(np.nan, index=d.index)
    pos[d["close"] >= hh] = 1.0
    pos[d["close"] <= ll] = -1.0
    return pos.ffill().fillna(0.0)

def sig_tsmom(d, n=60):
    """시계열 모멘텀: n봉 전 대비 상승→롱, 하락→숏."""
    return np.sign(d["close"] - d["close"].shift(n)).fillna(0.0)

def sig_macd(d):
    ef = d["close"].ewm(span=12, adjust=False).mean()
    es = d["close"].ewm(span=26, adjust=False).mean()
    macd = ef - es
    sigl = macd.ewm(span=9, adjust=False).mean()
    return np.sign(macd - sigl)


def _rsi(c, n=14):
    delta = c.diff()
    g = delta.clip(lower=0); l = (-delta).clip(lower=0)
    ag = g.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
    al = l.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
    return 100 - 100/(1 + ag/al.replace(0, np.nan))


def backtest_vec(d, pos, fee=FEE, bars_per_year=BARS_PER_YEAR_1M):
    """벡터화 백테스트. pos: 목표 포지션(+1/0/-1), 다음봉 수익에 적용."""
    ret = d["close"].pct_change().fillna(0.0)
    pos = pos.shift(1).fillna(0.0)                     # 신호는 다음봉부터 유효
    turnover = pos.diff().abs().fillna(pos.abs())      # 포지션 변경량
    strat = pos * ret - turnover * fee                 # 수수료 차감 순수익
    eq = (1 + strat).cumprod()
    total = eq.iloc[-1] - 1
    sharpe = (strat.mean()/strat.std()*np.sqrt(bars_per_year)) if strat.std() > 0 else 0.0
    dd = (eq/eq.cummax() - 1).min()
    trades = int((pos.diff().abs() > 0).sum())
    return {"ret": total*100, "sharpe": sharpe, "mdd": dd*100, "trades": trades}


STRATS = {
    "B&H 매수보유":      (sig_buyhold, {}),
    "S&H 매도보유":      (sig_shorthold, {}),
    "MeanRev BB+RSI":    (sig_mr_bb_rsi, {}),
    "EMA교차(20/60)":    (sig_ema_cross, {}),
    "Donchian돌파(60)":  (sig_donchian, {"n": 60}),
    "TS모멘텀(60)":      (sig_tsmom, {"n": 60}),
    "MACD추세":          (sig_macd, {}),
}
TFS = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h"}


def main():
    raw = load_1m()
    span_days = (raw["dt"].iloc[-1] - raw["dt"].iloc[0]).total_seconds()/86400
    bh = raw["close"].iloc[-1]/raw["close"].iloc[0] - 1
    print(f"[데이터] {len(raw):,}봉 1m, {span_days:.0f}일 "
          f"({raw['dt'].iloc[0].date()}~{raw['dt'].iloc[-1].date()})")
    print(f"[벤치마크] 기간 BTC 매수보유(무수수료) 수익률: {bh*100:+.1f}%\n")

    print(f"{'TF':>4} {'전략':<18} | {'학습 ret':>9} {'shrp':>6} | {'검증 ret':>9} {'shrp':>6} {'거래':>6}  판정")
    print("-"*86)
    results = []
    for tfname, rule in TFS.items():
        d = resample(raw, rule)
        bpy = BARS_PER_YEAR_1M / {"5min":5,"15min":15,"1h":60,"4h":240}[rule]
        split = int(len(d)*0.7)
        d_is, d_oos = d.iloc[:split].reset_index(drop=True), d.iloc[split:].reset_index(drop=True)
        for name, (fn, kw) in STRATS.items():
            try:
                r_is  = backtest_vec(d_is,  fn(d_is, **kw),  bars_per_year=bpy)
                r_oos = backtest_vec(d_oos, fn(d_oos, **kw), bars_per_year=bpy)
            except Exception as e:
                print(f"{tfname:>4} {name:<18} ERROR {e}")
                continue
            # 판정: 검증구간 Sharpe>0.5 이고 양수익이면 robust 후보
            robust = "✅robust" if (r_oos["sharpe"] > 0.5 and r_oos["ret"] > 0) else (
                     "~약함" if r_oos["ret"] > 0 else "✗손실")
            print(f"{tfname:>4} {name:<18} | {r_is['ret']:+8.1f}% {r_is['sharpe']:+5.1f} | "
                  f"{r_oos['ret']:+8.1f}% {r_oos['sharpe']:+5.1f} {r_oos['trades']:>6}  {robust}")
            results.append((tfname, name, r_oos))
        print("-"*86)

    # 검증구간 기준 상위 5
    top = sorted(results, key=lambda x: x[2]["sharpe"], reverse=True)[:5]
    print("\n[검증구간 Sharpe 상위 5]")
    for tf, nm, r in top:
        print(f"  {tf:>4} {nm:<18} OOS: {r['ret']:+7.1f}%  Sharpe {r['sharpe']:+.2f}  MDD {r['mdd']:.1f}%  거래 {r['trades']}")


if __name__ == "__main__":
    main()
