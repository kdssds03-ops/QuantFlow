"""scripts/test_portfolio.py — worker.portfolio 사이징 단위검증 (DB 없이 합성데이터).

실행(컨테이너 내):  docker compose run --rm worker python scripts/test_portfolio.py
또는 numpy/pandas 있는 환경:  python scripts/test_portfolio.py
"""
import sys
from pathlib import Path
import numpy as np

sys.path.append(str(Path(__file__).resolve().parent.parent))
from worker.portfolio import portfolio_effective_risk  # noqa: E402

STEP = 4 * 3600 * 1000


def make_rows(n_4h, step_std, seed):
    rng = np.random.default_rng(seed)
    px = 100 * np.cumprod(1 + rng.normal(0, step_std, n_4h))
    return [(i * STEP, float(px[i])) for i in range(n_4h)]


STEPVOL = {"BTC/USDT": 0.015, "ETH/USDT": 0.025, "SOL/USDT": 0.04, "BNB/USDT": 0.02}
SYMS = list(STEPVOL)
DATA = {s: make_rows(120, STEPVOL[s], i + 1) for i, s in enumerate(SYMS)}


def fetch(sym, need):
    return DATA[sym]


def main():
    print("[T1] 정상 사이징 (k=1, max_gross=1.5, cap=0.57): 저변동>고변동 가중 + 총gross≤cap")
    eff = {s: float(portfolio_effective_risk(
        s, symbols=SYMS, fetch_1m_closes=fetch, n_bars=30, target_vol=0.15,
        max_lev=3.0, k=1.0, max_gross=1.5, per_symbol_cap=0.57)) for s in SYMS}
    for s in SYMS:
        print(f"   {s:<10} {eff[s]:.4f}")
    assert all(v > 0 for v in eff.values())
    assert eff["BTC/USDT"] > eff["SOL/USDT"]
    assert sum(eff.values()) <= 1.5 + 1e-6

    print("[T2] max_gross 하드캡: k=3.5에도 총노출 ≤1.5x")
    eff2 = {s: float(portfolio_effective_risk(
        s, symbols=SYMS, fetch_1m_closes=fetch, n_bars=30, target_vol=0.15,
        max_lev=3.0, k=3.5, max_gross=1.5, per_symbol_cap=0.57)) for s in SYMS}
    assert sum(eff2.values()) <= 1.5 + 1e-6

    print("[T3] 심볼당 방화벽 클램프 (cap=0.30)")
    eff3 = {s: float(portfolio_effective_risk(
        s, symbols=SYMS, fetch_1m_closes=fetch, n_bars=30, target_vol=0.15,
        max_lev=3.0, k=3.5, max_gross=3.0, per_symbol_cap=0.30)) for s in SYMS}
    assert max(eff3.values()) <= 0.30 + 1e-9

    print("[T4] 데이터 부족 → 0 (워밍업 안전)")
    assert float(portfolio_effective_risk(
        "BTC/USDT", symbols=SYMS, fetch_1m_closes=lambda s, n: DATA[s][:10],
        n_bars=30, k=1.0, max_gross=1.5, per_symbol_cap=0.57)) == 0.0

    print("[T5] 예외 안전 → 0 (진입 보류)")

    def boom(s, n):
        raise RuntimeError("DB down")
    assert float(portfolio_effective_risk(
        "BTC/USDT", symbols=SYMS, fetch_1m_closes=boom,
        n_bars=30, k=1.0, max_gross=1.5, per_symbol_cap=0.57)) == 0.0

    print("\n🎉 전 항목 통과")


if __name__ == "__main__":
    main()
