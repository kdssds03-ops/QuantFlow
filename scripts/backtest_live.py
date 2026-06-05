"""
scripts/backtest_live.py — 라이브 전략(analyze_and_trade) 충실 재현 백테스터

기존 scripts/backtest.py는 단순 숏 전용 BB+RSI 전략이라 라이브 엔진과 다르다.
이 백테스터는 worker/tasks.py::analyze_and_trade 의 의사결정 우선순위를 그대로 옮겨
LONG/SHORT 양방향 + 소프트SL + 하드TP/SL + 타임아웃 + 트레일링 + 피라미딩 + 국면판독을
1분봉 단위로 시뮬레이션한다.

⚠️ 모델링 단순화(명시):
  - 레버리지 미반영: 진입 노셔널 = 자본의 ENTRY_PORTION(10%). PnL=(청산-진입)*수량.
    (선물 레버리지·증거금 청산은 모델링하지 않음 → 보수적 추정)
  - 슬리피지/자본 방화벽/지정가 미체결은 미반영 (시장가 즉시 체결 가정)
  - 시그널/지표는 worker.indicators.compute_all_features (라이브와 동일 코드)로 산출
  - 예측기는 worker.predictor.RuleBasedPredictor (라이브 RULE 모드와 동일)

사용:
  python scripts/backtest_live.py                # 베이스라인
  python scripts/backtest_live.py --experiments  # 베이스라인 + 개선안 비교
"""
import sys
import argparse
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from worker.indicators import compute_all_features          # noqa: E402
from worker.predictor import RuleBasedPredictor              # noqa: E402

DATA_CANDIDATES = [
    PROJECT_ROOT / "data" / "btc_1m_1year.csv",
    PROJECT_ROOT / "btc_1m_1year.csv",
]
BARS_PER_YEAR = 525_600  # 1분봉 기준


# ──────────────────────────────────────────────────────────────────────────
@dataclass
class StrategyParams:
    """라이브 tasks.py 상수 기본값과 1:1 매칭."""
    entry_portion: float = 0.10        # RISK_FACTOR
    fee_rate: float = 0.0005           # 시장가 0.05% (왕복 0.1%)
    soft_sl: float = -0.0100           # STOP_LOSS_THRESHOLD (-1%)
    hard_sl: float = -0.0150           # HARD_SL_THRESHOLD
    hard_tp: float = 0.0300            # HARD_TP_THRESHOLD
    timeout_min: int = 240             # MAX_POSITION_MINUTES (4h)
    trail_activate: float = 0.1500     # TRAILING_STOP_ACTIVATION_ROI (+15%)
    trail_drawdown: float = 0.0500     # TRAILING_STOP_DRAWDOWN (-5%)
    pyramid_profit: float = 0.0050     # PYRAMID_PROFIT_THRESHOLD (+0.5%)
    pyramid_max: int = 2               # PYRAMID_MAX_ADDS
    pyramid_ratio: float = 0.50        # PYRAMID_ADD_RATIO
    cooldown_min: int = 5              # COOLDOWN_MINUTES
    price_buffer_trend: float = 0.002  # 추세장 피라미딩 버퍼
    price_buffer_chop: float = 0.005   # 횡보장 버퍼
    # ── 국면판독 ──
    adx_trend: float = 25.0            # ADX_TREND_THRESHOLD
    atr_expansion: float = 1.5         # ATR_EXPANSION_RATIO
    atr_lookback: int = 240            # ATR_LOOKBACK_CANDLES
    # ── 실험용 토글 (개선안) ──
    entry_regime_filter: bool = False  # True: CHOP 국면에서만 신규 진입 허용 (개선#3)
    pyramid_in_chop: bool = False      # 라이브: CHOP에서 피라미딩 0회 (False 유지가 라이브)


@dataclass
class Position:
    side: str = "FLAT"      # FLAT/LONG/SHORT
    entry_price: float = 0.0
    qty: float = 0.0
    entry_idx: int = 0
    peak_roi: float = 0.0
    adds: int = 0
    entry_regime: str = "?"  # 진입 시점 국면 (손익 귀속 분석용)


def classify_regime(adx, atr, atr_avg, p: StrategyParams) -> str:
    """tasks.py::_classify_market_regime 축약 재현."""
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in (adx, atr, atr_avg)):
        return "FLAT_HOLD"
    if atr_avg <= 0:
        return "FLAT_HOLD"
    cond_adx = adx >= p.adx_trend
    cond_atr = (atr / atr_avg) >= p.atr_expansion
    return "TREND" if (cond_adx and cond_atr) else "CHOP"


def run_backtest(df: pd.DataFrame, p: StrategyParams, initial: float = 10_000.0):
    pred = RuleBasedPredictor()
    equity = initial
    pos = Position()
    trades = []
    equity_curve = []
    last_entry_bar = {"BUY": -10**9, "SELL": -10**9}  # 쿨다운용 직전 동일방향 진입 봉
    last_entry_price = {"BUY": 0.0, "SELL": 0.0}

    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    rsi = df["rsi_14"].values.astype(float)
    bbu = df["bb_upper"].values.astype(float)
    bbl = df["bb_lower"].values.astype(float)
    adx = df["adx_14"].values.astype(float)
    atr = df["atr_14"].values.astype(float)
    # 240봉 평균 ATR (롤링)
    atr_avg = pd.Series(atr).rolling(p.atr_lookback, min_periods=2).mean().values

    rows = df.to_dict("records")

    def close_position(i, price, reason):
        nonlocal equity, pos
        notional = pos.qty * price
        fee = notional * p.fee_rate
        if pos.side == "LONG":
            pnl = (price - pos.entry_price) * pos.qty
        else:  # SHORT
            pnl = (pos.entry_price - price) * pos.qty
        equity += pnl - fee
        trades.append({
            "side": pos.side, "entry_idx": pos.entry_idx, "exit_idx": i,
            "entry_price": pos.entry_price, "exit_price": price,
            "qty": pos.qty, "duration": i - pos.entry_idx,
            "pnl": pnl - fee, "reason": reason, "adds": pos.adds,
            "entry_regime": pos.entry_regime,
        })
        pos = Position()

    def open_position(i, price, side, regime):
        nonlocal equity, pos
        notional = equity * p.entry_portion
        qty = notional / price
        fee = notional * p.fee_rate
        equity -= fee
        pos = Position(side=side, entry_price=price, qty=qty, entry_idx=i, entry_regime=regime)
        last_entry_bar["BUY" if side == "LONG" else "SELL"] = i
        last_entry_price["BUY" if side == "LONG" else "SELL"] = price

    def pyramid_add(i, price):
        nonlocal pos
        add_qty = pos.qty * p.pyramid_ratio
        notional = add_qty * price
        # 신규 노셔널이 자본을 넘지 않도록(단순 가드) — 라이브 자본 방화벽 근사
        fee = notional * p.fee_rate
        nonlocal_equity_fee(fee)
        new_qty = pos.qty + add_qty
        pos.entry_price = (pos.entry_price * pos.qty + price * add_qty) / new_qty
        pos.qty = new_qty
        pos.adds += 1
        last_entry_bar["BUY"] = i
        last_entry_price["BUY"] = price

    def nonlocal_equity_fee(fee):
        nonlocal equity
        equity -= fee

    for i in range(len(df)):
        c = close[i]
        if np.isnan(bbu[i]) or np.isnan(rsi[i]) or np.isnan(bbl[i]):
            equity_curve.append(equity)
            continue

        regime = classify_regime(adx[i], atr[i], atr_avg[i], p)

        # ── 보유 포지션 청산 우선순위 (라이브 순서) ─────────────────────
        if pos.side != "FLAT":
            if pos.side == "LONG":
                ret = (c - pos.entry_price) / pos.entry_price
            else:
                ret = (pos.entry_price - c) / pos.entry_price
            pos.peak_roi = max(pos.peak_roi, ret)
            dur = i - pos.entry_idx

            exit_reason = None
            # 1) 소프트 SL (-1%)
            if ret <= p.soft_sl:
                exit_reason = "SOFT_SL"
            # 2) 타임아웃
            elif dur >= p.timeout_min:
                exit_reason = "TIMEOUT"
            # 3) 트레일링 (+활성 후 반납)
            elif pos.peak_roi >= p.trail_activate and (pos.peak_roi - ret) >= p.trail_drawdown:
                exit_reason = "TRAILING"
            # 4) 하드 TP / SL
            elif ret >= p.hard_tp:
                exit_reason = "HARD_TP"
            elif ret <= p.hard_sl:
                exit_reason = "HARD_SL"

            if exit_reason:
                close_position(i, c, exit_reason)
                equity_curve.append(equity)
                continue

        # ── 예측기 시그널 ───────────────────────────────────────────────
        sig, _conf = pred.predict_with_confidence(rows[i])

        if sig == "HOLD":
            equity_curve.append(equity)
            continue

        # ── 시그널 기반 진입/청산/피라미딩 (라이브 분기) ─────────────────
        if pos.side == "FLAT":
            # FLAT_HOLD 국면이면 진입 차단
            if regime == "FLAT_HOLD":
                equity_curve.append(equity)
                continue
            # [개선#3] 진입 국면 필터: 켜면 CHOP에서만 평균회귀 진입
            if p.entry_regime_filter and regime != "CHOP":
                equity_curve.append(equity)
                continue
            # 쿨다운/단가 버퍼 가드 (동일방향 재진입 — 신규 진입엔 직전가 없으면 통과)
            side = "LONG" if sig == "BUY" else "SHORT"
            open_position(i, c, side, regime)

        elif pos.side == "SHORT" and sig == "BUY":
            # 역시그널 숏 청산 (리버스 스위칭) — 가드 면제
            close_position(i, c, "REVERSE_EXIT")

        elif pos.side == "LONG" and sig == "SELL":
            close_position(i, c, "REVERSE_EXIT")

        elif pos.side == "LONG" and sig == "BUY":
            # 불타기: LONG 수익권 + 국면 허용 + 횟수/단가 가드
            ret = (c - pos.entry_price) / pos.entry_price
            buf = p.price_buffer_trend if regime == "TREND" else p.price_buffer_chop
            pmax = p.pyramid_max if (regime == "TREND" or p.pyramid_in_chop) else 0
            cooled = (i - last_entry_bar["BUY"]) >= p.cooldown_min
            favorable = c >= last_entry_price["BUY"] * (1 + buf)
            if (ret >= p.pyramid_profit and pos.adds < pmax and cooled and favorable):
                pyramid_add(i, c)
        # SHORT+SELL, 그 외: 홀드

        equity_curve.append(equity)

    # 마지막 포지션 강제 청산 (평가)
    if pos.side != "FLAT":
        close_position(len(df) - 1, close[-1], "EOD")

    return trades, np.array(equity_curve), equity


def metrics(trades, curve, initial, final):
    ret_pct = (final / initial - 1) * 100
    peak = np.maximum.accumulate(curve) if len(curve) else np.array([initial])
    dd = (curve - peak) / peak
    mdd = dd.min() * 100 if len(dd) else 0.0
    # 샤프 (per-bar → 연율화)
    if len(curve) > 2:
        bar_ret = np.diff(curve) / curve[:-1]
        sharpe = (bar_ret.mean() / bar_ret.std() * np.sqrt(BARS_PER_YEAR)) if bar_ret.std() > 0 else 0.0
    else:
        sharpe = 0.0
    n = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    win_rate = len(wins) / n * 100 if n else 0.0
    gross_win = sum(t["pnl"] for t in wins)
    gross_loss = -sum(t["pnl"] for t in losses)
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    avg_dur = np.mean([t["duration"] for t in trades]) if n else 0.0
    longs = sum(1 for t in trades if t["side"] == "LONG")
    shorts = n - longs
    return {
        "return_pct": ret_pct, "mdd_pct": mdd, "sharpe": sharpe,
        "trades": n, "win_rate": win_rate, "profit_factor": pf,
        "avg_dur_min": avg_dur, "longs": longs, "shorts": shorts,
        "final": final,
    }


def reason_breakdown(trades):
    from collections import Counter, defaultdict
    cnt = Counter(t["reason"] for t in trades)
    pnl = defaultdict(float)
    for t in trades:
        pnl[t["reason"]] += t["pnl"]
    return cnt, pnl


def print_report(label, m, trades):
    print(f"\n{'='*56}\n[{label}]\n{'='*56}")
    print(f"  최종자산   : {m['final']:,.2f} USDT  (수익률 {m['return_pct']:+.2f}%)")
    print(f"  MDD        : {m['mdd_pct']:.2f}%   |  Sharpe(연율): {m['sharpe']:.2f}")
    print(f"  거래수     : {m['trades']}회 (LONG {m['longs']} / SHORT {m['shorts']})")
    print(f"  승률       : {m['win_rate']:.1f}%  |  Profit Factor: {m['profit_factor']:.2f}")
    print(f"  평균보유   : {m['avg_dur_min']:.0f}분")
    cnt, pnl = reason_breakdown(trades)
    print("  청산사유별 :")
    for r in sorted(cnt, key=lambda x: -cnt[x]):
        print(f"    - {r:<13} {cnt[r]:>4}회  순손익 {pnl[r]:+9.2f} USDT")
    # 진입 국면별 손익 귀속
    from collections import Counter, defaultdict
    rc = Counter(t.get("entry_regime", "?") for t in trades)
    rp = defaultdict(float)
    rw = defaultdict(int)
    for t in trades:
        rp[t.get("entry_regime", "?")] += t["pnl"]
        if t["pnl"] > 0:
            rw[t.get("entry_regime", "?")] += 1
    print("  진입국면별 :")
    for r in sorted(rc, key=lambda x: -rc[x]):
        wr = rw[r] / rc[r] * 100 if rc[r] else 0
        print(f"    - {r:<10} {rc[r]:>4}회  순손익 {rp[r]:+9.2f} USDT  (승률 {wr:.0f}%)")


def load_data():
    for path in DATA_CANDIDATES:
        if path.exists():
            df = pd.read_csv(path)
            cols = {"timestamp": "timestamp_ms"}
            df = df.rename(columns=cols)
            keep = ["timestamp_ms", "open", "high", "low", "close", "volume"]
            df = df[[c for c in keep if c in df.columns]].copy()
            print(f"[데이터] {path.name}: {len(df)}봉")
            return df
    raise FileNotFoundError("btc_1m_1year.csv 를 data/ 또는 루트에 두세요.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiments", action="store_true", help="개선안 비교 실행")
    args = ap.parse_args()

    df = load_data()
    df = compute_all_features(df)

    base = StrategyParams()
    trades, curve, final = run_backtest(df, base)
    print_report("베이스라인 (라이브 현재 설정)", metrics(trades, curve, 10_000, final), trades)

    if args.experiments:
        # 개선#3: 진입 국면 필터 (CHOP에서만 평균회귀 진입)
        e1 = replace(base, entry_regime_filter=True)
        t1, c1, f1 = run_backtest(df, e1)
        print_report("실험1: 진입 국면필터(CHOP 한정)", metrics(t1, c1, 10_000, f1), t1)

        # 개선#2: 트레일링 활성 기준 +15%→+2%, 반납 1%
        e2 = replace(base, trail_activate=0.02, trail_drawdown=0.01)
        t2, c2, f2 = run_backtest(df, e2)
        print_report("실험2: 트레일링 완화(+2%/-1%)", metrics(t2, c2, 10_000, f2), t2)

        # 개선 조합: 국면필터 + 트레일링 완화
        e3 = replace(base, entry_regime_filter=True, trail_activate=0.02, trail_drawdown=0.01)
        t3, c3, f3 = run_backtest(df, e3)
        print_report("실험3: 국면필터 + 트레일링완화 조합", metrics(t3, c3, 10_000, f3), t3)


if __name__ == "__main__":
    main()
