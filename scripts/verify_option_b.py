"""
scripts/verify_option_b.py — "B전략(분산 + 낙폭예산만큼 레버리지)"이 실제로
단일 BTC보다 수익을 높이는지 정직하게 검증.

핵심 질문: 분산 포트(리스크패리티+변동성타겟)는 MDD가 낮으니(-15%),
단일 BTC의 낙폭(-50%)까지 레버리지를 키우면 수익이 올라갈까?

정직성 장치:
  1) 레버리지 배율 L은 **학습구간(IS, 앞 60%)에서만** 결정한다.
     - IS에서 분산포트의 MDD가 단일 BTC의 IS MDD와 같아지도록 L을 이분탐색.
     - OOS 데이터를 전혀 보지 않고 정함 → look-ahead 차단.
  2) 그 L을 **검증구간(OOS, 뒤 40%)에 적용**해 실제 CAGR·MDD·Sharpe를 측정.
  3) 단일 BTC OOS와 risk-matched(낙폭 동일 예산) 비교.

이론적 주의: Sharpe는 레버리지 불변(수익·변동성·낙폭이 같은 배율로 커짐).
따라서 OOS Sharpe가 단일 BTC보다 높지 않으면, 레버리지로 수익을 키워도
낙폭이 같이 커져 'free lunch'는 없다. 이 스크립트는 그걸 수치로 확인한다.
"""
import sys
from pathlib import Path
import numpy as np

# Windows cp949 콘솔에서 em-dash/이모지 출력 시 죽지 않도록 UTF-8 강제
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.portfolio_backtest import (
    load_basket, trend_returns, build_portfolios, stats, BPY,
)


def mdd_of(returns, lev=1.0):
    """수익 시리즈에 레버리지 lev를 곱했을 때의 최대낙폭(MDD, 음수%)."""
    eq = (1 + lev * returns).cumprod()
    return float((eq / eq.cummax() - 1).min()) * 100


def solve_leverage(returns, target_mdd):
    """returns에 곱할 레버리지 L을 찾는다: MDD(L) ≈ target_mdd (둘 다 음수%).
    MDD는 L에 대해 단조 악화(더 음수)이므로 이분탐색."""
    lo, hi = 0.01, 20.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if mdd_of(returns, mid) > target_mdd:   # 낙폭이 아직 덜함(덜 음수) → 더 키움
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def full_stats(returns, lev, label):
    s = (lev * returns).dropna()
    return stats(s, label)


def main():
    px = load_basket()
    print(f"[데이터] {len(px)}개 4h봉, {px.index[0].date()} ~ {px.index[-1].date()}, "
          f"{len(px.columns)}코인\n")

    R = trend_returns(px)
    btc = R["BTC/USDT"]
    div = build_portfolios(R)["리스크패리티+변동성타겟"]

    split = int(len(R) * 0.6)
    btc_is, btc_oos = btc.iloc[:split], btc.iloc[split:]
    div_is, div_oos = div.iloc[:split], div.iloc[split:]

    # ── STEP 1: 레버리지를 IS에서만 결정 (단일 BTC의 IS 낙폭에 맞춤) ──────
    btc_is_mdd = mdd_of(btc_is, 1.0)
    L = solve_leverage(div_is, btc_is_mdd)
    print("=" * 88)
    print("STEP 1 — 레버리지 결정 (학습구간 IS에서만, OOS 미사용)")
    print("=" * 88)
    print(f"  단일 BTC 학습구간 MDD       : {btc_is_mdd:6.1f}%")
    print(f"  분산포트 학습구간 MDD(L=1)  : {mdd_of(div_is, 1.0):6.1f}%")
    print(f"  → 분산포트에 적용할 레버리지 : {L:.2f}x  "
          f"(이때 분산포트 IS MDD = {mdd_of(div_is, L):.1f}%, BTC와 매칭)\n")

    # ── STEP 2: 그 레버리지를 OOS에 적용해 비교 ──────────────────────────
    print("=" * 88)
    print("STEP 2 — 검증구간(OOS)에서 risk-matched 비교  ← 진짜 판정")
    print("=" * 88)
    b = full_stats(btc_oos, 1.0, "단일 BTC (L=1)")
    d1 = full_stats(div_oos, 1.0, "분산포트 (L=1, 레버 전)")
    dL = full_stats(div_oos, L,   f"분산포트 (L={L:.2f}, 레버 후)")
    hdr = f"{'전략':<26}{'CAGR':>9}{'MDD':>9}{'Sharpe':>9}{'총수익':>10}"
    print(hdr)
    print("-" * 88)
    for x in (b, d1, dL):
        print(f"{x['label']:<26}{x['cagr']:>8.1f}%{x['mdd']:>8.0f}%"
              f"{x['sharpe']:>9.2f}{x['ret']:>9.0f}%")
    print("=" * 88)

    # ── 판정 ────────────────────────────────────────────────────────────
    print("\n[판정]")
    print(f"  OOS에서 분산포트를 단일 BTC와 같은 낙폭({b['mdd']:.0f}% 수준)까지 "
          f"레버리지({L:.2f}x) 키웠을 때:")
    print(f"    · CAGR  {b['cagr']:.1f}% (BTC)  vs  {dL['cagr']:.1f}% (분산+레버)")
    print(f"    · MDD   {b['mdd']:.0f}% (BTC)  vs  {dL['mdd']:.0f}% (분산+레버)")
    if dL['cagr'] > b['cagr'] * 1.10 and dL['mdd'] >= b['mdd'] * 1.10:
        verdict = "✅ B전략 유효 — 같은 낙폭에서 수익이 유의미하게 높음."
    elif dL['cagr'] > b['cagr']:
        verdict = "△ 미미한 우위 — 수익은 약간 높으나 차이가 작아 실전 비용/위험 고려 시 불확실."
    else:
        verdict = "❌ B전략 무효 — 레버리지로 키워도 단일 BTC를 못 넘음 (free lunch 없음)."
    print(f"  → {verdict}")
    print(f"\n  참고: OOS Sharpe  BTC {b['sharpe']:.2f}  vs  분산 {d1['sharpe']:.2f} "
          f"(Sharpe는 레버리지 불변 — 이 값이 낮으면 레버로도 못 이김)")


if __name__ == "__main__":
    main()
