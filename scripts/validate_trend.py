"""추세추종 후보 심층 검증: 롱/숏 분리손익 + 월별 일관성 (방향운 vs 진짜엣지 판별)."""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
from scripts.signal_research import (  # noqa: E402
    load_1m, resample, backtest_vec, sig_ema_cross, sig_donchian, sig_tsmom, FEE, BARS_PER_YEAR_1M
)

raw = load_1m()

def split_long_short(d, pos, fee=FEE):
    """롱 레그/숏 레그 각각의 순수익 분리."""
    ret = d["close"].pct_change().fillna(0.0)
    pos = pos.shift(1).fillna(0.0)
    turn = pos.diff().abs().fillna(pos.abs())
    pnl = pos*ret - turn*fee
    long_pnl = pnl[pos > 0].sum()*100
    short_pnl = pnl[pos < 0].sum()*100
    long_bars = int((pos > 0).sum()); short_bars = int((pos < 0).sum())
    return long_pnl, short_pnl, long_bars, short_bars

def monthly_consistency(d, pos, fee=FEE):
    """월별 순수익(%) — 여러 구간에서 꾸준한지 확인."""
    ret = d["close"].pct_change().fillna(0.0)
    pos_s = pos.shift(1).fillna(0.0)
    turn = pos_s.diff().abs().fillna(pos_s.abs())
    pnl = pos_s*ret - turn*fee
    tmp = pd.DataFrame({"dt": d["dt"], "pnl": pnl}).set_index("dt")
    monthly = (tmp["pnl"].resample("ME").sum()*100)
    return monthly

CANDS = {
    "1h EMA교차(20/60)": ("1h", lambda d: sig_ema_cross(d, 20, 60)),
    "4h EMA교차(20/60)": ("4h", lambda d: sig_ema_cross(d, 20, 60)),
    "1h Donchian(60)":   ("1h", lambda d: sig_donchian(d, 60)),
    "4h Donchian(60)":   ("4h", lambda d: sig_donchian(d, 60)),
    "1h TS모멘텀(60)":    ("1h", lambda d: sig_tsmom(d, 60)),
}
RULE = {"1h": "1h", "4h": "4h"}
BPY = {"1h": BARS_PER_YEAR_1M/60, "4h": BARS_PER_YEAR_1M/240}

print(f"기간 BTC 매수보유: {(raw['close'].iloc[-1]/raw['close'].iloc[0]-1)*100:+.1f}% (하락장)\n")
for name, (tf, fn) in CANDS.items():
    d = resample(raw, RULE[tf])
    pos = fn(d)
    full = backtest_vec(d, pos, bars_per_year=BPY[tf])
    lp, sp, lb, sb = split_long_short(d, pos)
    print(f"=== {name} ===")
    print(f"  전체(180일): {full['ret']:+.1f}%  Sharpe {full['sharpe']:+.2f}  MDD {full['mdd']:.1f}%  거래 {full['trades']}")
    print(f"  롱 레그: {lp:+.1f}% ({lb}봉)   숏 레그: {sp:+.1f}% ({sb}봉)")
    mon = monthly_consistency(d, pos)
    wins = (mon > 0).sum(); tot = len(mon)
    print(f"  월별: " + "  ".join(f"{m.strftime('%y-%m')}:{v:+.1f}%" for m, v in mon.items()))
    print(f"  → 흑자月 {wins}/{tot}  | 판정: " + (
        "롱·숏 양쪽 흑자=엣지 가능성" if (lp > 0 and sp > 0) else
        "숏만 흑자=하락장 방향운 의심") + "\n")
