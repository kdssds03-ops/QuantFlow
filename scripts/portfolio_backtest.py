"""
scripts/portfolio_backtest.py — 분산 + 변동성타게팅 추세 포트폴리오 검증

목적: 단일 BTC 추세추종(Sharpe ~0.5-0.7)을 '여러 코인 분산 + 리스크패리티 +
변동성타게팅'으로 개선할 수 있는지 학습/검증(IS/OOS) 분리로 엄격 검증.
개선이 OOS에서도 확인돼야만 라이브 반영(과최적 방지).

전략(코인별 동일): 4h EMA(30/60) 교차 → 항상 롱/숏 (검증된 설정, 재튜닝 안 함)
설계 레이어:
  1) 동일가중           : 단순 평균
  2) 리스크패리티        : 변동성 역수 가중 (각 코인 위험기여 동일) — 파라미터 없음
  3) + 변동성타게팅      : 포트 전체를 목표 연변동성으로 스케일 (드로다운 통제)
수수료 0.05%(편도). 모든 vol 추정은 1봉 지연(look-ahead 차단).
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
import ccxt

sys.path.append(str(Path(__file__).resolve().parent.parent))

FEE = 0.0005
BPY = 365 * 6           # 4h봉/년
TARGET_VOL = 0.15       # 변동성타게팅 목표 연변동성 15%
VOL_WIN = 30            # 변동성 추정 롤링 윈도 (4h봉, ≈5일)
MAX_LEV = 3.0           # 변동성타게팅 레버리지 상한 (안전)
CACHE = Path(__file__).resolve().parent.parent / "data" / "_basket_4h.json"
BASKET = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
          "XRP/USDT", "DOGE/USDT", "AVAX/USDT", "LINK/USDT"]


def fetch_4h(ex, symbol, days=1095):
    end = ex.milliseconds(); since = end - days * 86400 * 1000
    rows, cur = [], since
    while cur < end:
        b = ex.fetch_ohlcv(symbol, "4h", since=cur, limit=1500)
        if not b:
            break
        rows.extend(b)
        nxt = b[-1][0] + 4 * 3600 * 1000
        if nxt <= cur:
            break
        cur = nxt
        time.sleep(0.05)
    return rows


def load_basket():
    if CACHE.exists():
        raw = json.loads(CACHE.read_text())
        print(f"[캐시] {CACHE.name} 로드")
    else:
        ex = ccxt.binanceusdm({"enableRateLimit": True})
        raw = {}
        for s in BASKET:
            raw[s] = fetch_4h(ex, s)
            print(f"  수집 {s}: {len(raw[s])}봉")
        CACHE.write_text(json.dumps(raw))
        print(f"[캐시] {CACHE.name} 저장")
    # close 시리즈 정렬 (공통 타임스탬프 교집합)
    closes = {}
    for s, rows in raw.items():
        df = pd.DataFrame(rows, columns=["ts", "o", "h", "l", "c", "v"]).drop_duplicates("ts")
        closes[s] = pd.Series(df["c"].values, index=pd.to_datetime(df["ts"], unit="ms", utc=True))
    px = pd.DataFrame(closes).dropna()
    return px


def trend_returns(px):
    """코인별 4h EMA(30/60) 추세추종 순수익(수수료 차감) DataFrame."""
    rets = {}
    for s in px.columns:
        c = px[s]
        ef = c.ewm(span=30, adjust=False).mean()
        es = c.ewm(span=60, adjust=False).mean()
        pos = np.sign(ef - es).shift(1).fillna(0.0)
        r = c.pct_change().fillna(0.0)
        turn = pos.diff().abs().fillna(pos.abs())
        rets[s] = pos * r - turn * FEE
    return pd.DataFrame(rets)


def stats(s, label=""):
    s = s.dropna()
    eq = (1 + s).cumprod()
    tot = eq.iloc[-1] - 1
    sh = s.mean() / s.std() * np.sqrt(BPY) if s.std() > 0 else 0
    mdd = (eq / eq.cummax() - 1).min()
    yrs = len(s) / BPY
    cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 else 0
    return {"label": label, "ret": tot * 100, "cagr": cagr * 100, "sharpe": sh, "mdd": mdd * 100}


def build_portfolios(R):
    """동일가중 / 리스크패리티 / 리스크패리티+변동성타게팅 포트 수익 시리즈."""
    # 1) 동일가중
    eq = R.mean(axis=1)
    # 2) 리스크패리티 (각 코인 변동성 역수 가중, 1봉 지연)
    vol = R.rolling(VOL_WIN).std().shift(1)
    inv = 1.0 / vol
    w = inv.div(inv.sum(axis=1), axis=0)
    rp = (R * w).sum(axis=1)
    # 3) + 변동성타게팅 (포트 변동성을 목표로 스케일, 1봉 지연)
    pvol = rp.rolling(VOL_WIN).std().shift(1) * np.sqrt(BPY)
    lev = (TARGET_VOL / pvol).clip(upper=MAX_LEV).fillna(0.0)
    vt = rp * lev
    return {"동일가중": eq, "리스크패리티": rp, "리스크패리티+변동성타겟": vt}


def main():
    px = load_basket()
    print(f"[데이터] {len(px)}개 4h봉, {px.index[0].date()} ~ {px.index[-1].date()}, {len(px.columns)}코인\n")
    R = trend_returns(px)
    btc = R["BTC/USDT"]
    ports = build_portfolios(R)

    split = int(len(R) * 0.6)
    def show(name, s):
        f = stats(s, name)
        i = stats(s.iloc[:split]); o = stats(s.iloc[split:])
        flag = "✅견고" if (o["sharpe"] > btc.iloc[split:].pipe(lambda x:(x.mean()/x.std()*np.sqrt(BPY)))) else "~비슷/약함"
        print(f"{name:<22} 전체 Sharpe {f['sharpe']:+.2f} CAGR {f['cagr']:+5.1f}% MDD {f['mdd']:5.0f}% | "
              f"학습 {i['sharpe']:+.2f} / 검증 {o['sharpe']:+.2f}  {flag}")

    print("=" * 92)
    print(f"{'전략':<22} {'위험조정(Sharpe)·수익·낙폭':<40} {'학습/검증 분리'}")
    print("=" * 92)
    show("단일 BTC (기준)", btc)
    print("-" * 92)
    for name, s in ports.items():
        show(name, s)
    print("=" * 92)
    # 최종 비교 요약
    b = stats(btc); best = stats(ports["리스크패리티+변동성타겟"])
    print(f"\n핵심: 단일BTC Sharpe {b['sharpe']:.2f} (MDD {b['mdd']:.0f}%) "
          f"→ 분산+변동성타겟 Sharpe {best['sharpe']:.2f} (MDD {best['mdd']:.0f}%)")
    print("판정: 검증구간(OOS)에서도 Sharpe가 개선되면 라이브 반영 가치 있음.")


if __name__ == "__main__":
    main()
