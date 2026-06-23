"""
scripts/improve_research.py — '지표/전략 개선' 후보의 IS/OOS 견고성 검증 (읽기 전용, 라이브 미수정)

배경: 현행 라이브는 단일 BTC 4h EMA(30/60) 추세추종(PREDICTOR_TYPE=TREND, RISK_FACTOR=0.50).
질문: "거래 기준 지표를 더 정교화해 (절대)수익을 올릴 수 있나?"
답(이 스크립트의 결론): 신호 자체를 화려하게 만드는 길은 OOS에서 대부분 무너진다.
       견고하게 절대수익을 끌어올리는 길은 (a) 검증된 동일 신호를 저상관 코인 바스켓에
       분산 + (b) 포트폴리오 연속 변동성타게팅으로 MDD를 1/3로 줄인 뒤 (c) 리스크 예산
       안에서 레버(k)로 수익을 키우는 것. 모든 신호/vol 추정은 1봉 지연(look-ahead 차단).

데이터: data/_basket_4h.json (8코인 × 3년 4h, portfolio_backtest.py와 동일 캐시)
수수료: 0.05% 편도(turnover에만). IS/OOS = 60/40.

실행: python scripts/improve_research.py
"""
import json
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "data" / "_basket_4h.json"
FEE = 0.0005
BPY = 365 * 6            # 4h봉/년 = 2190
SPLIT = 0.60
VOL_WIN = 30
TARGET_VOL = 0.15
MAX_LEV = 3.0
BASKET = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
          "XRP/USDT", "DOGE/USDT", "AVAX/USDT", "LINK/USDT"]


def load():
    raw = json.loads(CACHE.read_text())
    closes, highs, lows = {}, {}, {}
    for s, rows in raw.items():
        df = pd.DataFrame(rows, columns=["ts", "o", "h", "l", "c", "v"]).drop_duplicates("ts")
        idx = pd.to_datetime(df["ts"], unit="ms", utc=True)
        closes[s] = pd.Series(df["c"].values, index=idx)
        highs[s] = pd.Series(df["h"].values, index=idx)
        lows[s] = pd.Series(df["l"].values, index=idx)
    return pd.DataFrame(closes).dropna(), pd.DataFrame(highs).dropna(), pd.DataFrame(lows).dropna()


def stats(s):
    s = s.dropna()
    if len(s) < 10 or s.std() == 0:
        return dict(ret=0, cagr=0, sharpe=0, mdd=0)
    eq = (1 + s).cumprod(); yrs = len(s) / BPY
    return dict(ret=(eq.iloc[-1] - 1) * 100, cagr=(eq.iloc[-1] ** (1 / yrs) - 1) * 100,
                sharpe=s.mean() / s.std() * np.sqrt(BPY), mdd=(eq / eq.cummax() - 1).min() * 100)


def ema_pos(c, f=30, s=60):
    ef = c.ewm(span=f, adjust=False).mean(); es = c.ewm(span=s, adjust=False).mean()
    return np.sign(ef - es).shift(1).fillna(0.0)


def donch_pos(h, l, c, n=60):
    hh = h.rolling(n).max().shift(1); ll = l.rolling(n).min().shift(1)
    pos = pd.Series(np.nan, index=c.index); pos[c >= hh] = 1.0; pos[c <= ll] = -1.0
    return pos.ffill().fillna(0.0).shift(1).fillna(0.0)


def pos_returns(c, pos):
    r = c.pct_change().fillna(0.0); turn = pos.diff().abs().fillna(pos.abs())
    return pos * r - turn * FEE


def vt_portfolio(R):
    """리스크패리티(변동성 역수 가중) + 포트 연속 변동성타게팅. 모두 1봉 지연."""
    vol = R.rolling(VOL_WIN).std().shift(1); inv = 1.0 / vol
    w = inv.div(inv.sum(axis=1), axis=0)
    rp = (R * w).sum(axis=1)
    pvol = rp.rolling(VOL_WIN).std().shift(1) * np.sqrt(BPY)
    lev = (TARGET_VOL / pvol).clip(upper=MAX_LEV).fillna(0.0)
    return rp * lev


def split3(s):
    k = int(len(s) * SPLIT)
    return stats(s), stats(s.iloc[:k]), stats(s.iloc[k:])


def main():
    px, H, L = load()
    print(f"[데이터] {len(px)}개 4h봉, {px.index[0].date()}~{px.index[-1].date()}, "
          f"{len(px.columns)}코인 (IS={int(SPLIT*100)}%/OOS={int((1-SPLIT)*100)}%)\n")

    R = pd.DataFrame({s: pos_returns(px[s], ema_pos(px[s])) for s in px.columns})

    print("=" * 100)
    print("1) 코인별 EMA(30/60) 단독 — 분산 '재료'. 단독 알트는 OOS 붕괴가 흔함(체리피킹 금지)")
    print("=" * 100)
    for s in BASKET:
        f, i, o = split3(R[s])
        print(f"  {s:<10} CAGR {f['cagr']:+7.1f}% Sharpe {f['sharpe']:+.2f} "
              f"MDD {f['mdd']:6.1f}% | IS {i['sharpe']:+.2f} / OOS {o['sharpe']:+.2f}")
    corr = R.corr().where(~np.eye(len(R.columns), dtype=bool)).stack().mean()
    print(f"\n  코인간 전략수익 평균상관 = {corr:.2f} (낮을수록 분산효과↑)")

    print("\n" + "=" * 100)
    print("2) 합치는 방법별 포트폴리오 (신호는 동일 EMA30/60 고정)")
    print("=" * 100)
    btc = R["BTC/USDT"]; vt = vt_portfolio(R)
    for name, s in [("단일 BTC (현행 기준선)", btc), ("8코인 동일가중", R.mean(axis=1)),
                    ("리스크패리티+변동성타겟", vt)]:
        f, i, o = split3(s)
        print(f"  {name:<22} CAGR {f['cagr']:+6.1f}% Sharpe {f['sharpe']:+.2f} "
              f"MDD {f['mdd']:6.1f}% | IS {i['sharpe']:+.2f}/OOS {o['sharpe']:+.2f} (OOS CAGR {o['cagr']:+.1f}%)")

    print("\n" + "=" * 100)
    print("3) 절대수익: '같은 MDD 예산'에서 레버 k로 얼마까지 버나 (VT 포트 × k)")
    print("=" * 100)
    base = abs(stats(btc)["mdd"])
    print(f"  기준선 BTC: CAGR {stats(btc)['cagr']:+.1f}% @ MDD {stats(btc)['mdd']:.1f}%\n")
    print(f"  {'k':>4} {'CAGR':>8} {'MDD':>8} {'Sharpe':>7} {'OOS CAGR':>9}")
    for k in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
        f, _, o = split3(vt * k)
        mark = " ✅MDD≤기준" if abs(f["mdd"]) <= base else ""
        print(f"  {k:>4.1f} {f['cagr']:>+7.1f}% {f['mdd']:>+7.1f}% {f['sharpe']:>+6.2f} {o['cagr']:>+8.1f}%{mark}")

    print("\n" + "=" * 100)
    print("4) EMA 파라미터 고원 — VT포트 OOS Sharpe (넓게 양수=견고, 한 칸만 튀면 과최적)")
    print("=" * 100)
    print("  fast\\slow " + "".join(f"{s:>8}" for s in [50, 60, 70, 80]))
    for f_ in [20, 25, 30, 35, 40]:
        line = f"  {f_:>8}  "
        for sl in [50, 60, 70, 80]:
            Rg = pd.DataFrame({s: pos_returns(px[s], ema_pos(px[s], f_, sl)) for s in px.columns})
            _, _, o = split3(vt_portfolio(Rg))
            line += f"{o['sharpe']:>+8.2f}"
        print(line)

    print("\n" + "=" * 100)
    print("5) 앙상블 점검: EMA + 돈치안 — 포트 OOS에서 이득 없음(직교성 부족, 추가 안 함)")
    print("=" * 100)
    for n in [55, 60, 65]:
        Rens = pd.DataFrame({s: pos_returns(px[s], (ema_pos(px[s]) + donch_pos(H[s], L[s], px[s], n)) / 2.0)
                             for s in px.columns})
        f, i, o = split3(vt_portfolio(Rens))
        print(f"  EMA+돈치안({n}): CAGR {f['cagr']:+6.1f}% Sharpe {f['sharpe']:+.2f} "
              f"MDD {f['mdd']:6.1f}% | IS {i['sharpe']:+.2f}/OOS {o['sharpe']:+.2f}")
    f, i, o = split3(vt)
    print(f"  (참고) EMA단독: CAGR {f['cagr']:+6.1f}% Sharpe {f['sharpe']:+.2f} "
          f"MDD {f['mdd']:6.1f}% | IS {i['sharpe']:+.2f}/OOS {o['sharpe']:+.2f}")
    print("\n[정직성] 백테스트는 실전보다 낙관적(알트 슬리피지·펀딩·청산 미반영). 레버는 양날.")


if __name__ == "__main__":
    main()
