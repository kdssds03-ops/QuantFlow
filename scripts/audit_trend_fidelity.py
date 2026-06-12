"""
scripts/audit_trend_fidelity.py — 라이브 TREND 엔진 vs 검증된 백테스트 괴리 정량화

목적 (2026-06-12 전체 코드 감사의 근거 산출):
  [A] 피라미딩 영향: 검증된 전략(portfolio_backtest)은 피라미딩 없음.
      backtest_live의 TREND 시뮬은 '포지션당 최대 2회 + 유리단가 가드'를 가정.
      그러나 라이브(tasks.py)는 ① Redis 카운터가 마지막 add 후 1시간이면 만료되어
      장기 보유 포지션에 무제한 추가매수가 가능하고, ② 5분 내에는 단가 가드의
      부등호가 시뮬과 반대(하락 시 추가)이며 5분 후엔 단가 조건이 아예 없고,
      ③ 자본 방화벽(20% 한도)을 우회한다.
      → 세 구성을 같은 데이터로 비교: 시뮬 피라미딩 / 피라미딩 없음 / 라이브 충실 재현.

  [B] EMA 워밍업 윈도 영향: 라이브 TrendFollowingPredictor는 1분봉 23,040개(≈ 4h봉 96개)만
      조회해 EMA(30/60)를 계산 → EMA60에 시드 편향 ≈ (1-2/61)^96 ≈ 4% 잔존.
      백테스트는 전체 이력으로 EMA 산출(완전 수렴). 4h봉별 신호 불일치율을 측정.

사용: .venv 파이썬으로 실행. 출력은 표준출력.
"""
import sys
from pathlib import Path
from dataclasses import replace

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from worker.indicators import compute_all_features                      # noqa: E402
from scripts.backtest_live import (                                      # noqa: E402
    StrategyParams, Position, classify_regime, _trend_signal_per_bar,
    run_backtest, metrics, load_data,
)


# ──────────────────────────────────────────────────────────────────────────
# [A-3] 라이브 충실 재현 러너 — run_backtest 사본에 라이브 피라미딩 로직 이식
#   · 카운터: 마지막 add 후 60분 경과 시 0으로 리셋 (Redis TTL 3600 재현)
#   · 단가 가드: 직전 BUY 후 5분 내면 '현재가 <= 직전가*(1-buf)' (라이브 부등호 그대로),
#                5분 경과 시 단가 조건 없음 (쿨다운 레코드가 사라지므로)
#   · 자본 방화벽: 없음 (라이브 피라미딩 경로는 방화벽 호출 전에 return)
# ──────────────────────────────────────────────────────────────────────────
def run_backtest_live_faithful(df: pd.DataFrame, p: StrategyParams, initial: float = 10_000.0):
    trend_sig = _trend_signal_per_bar(df)
    p = replace(p, soft_sl=-0.12, hard_sl=-0.15, hard_tp=100.0,
                timeout_min=10**9, trail_activate=100.0)
    equity = initial
    pos = Position()
    trades = []
    equity_curve = []
    last_entry_bar = {"BUY": -10**9, "SELL": -10**9}
    last_entry_price = {"BUY": 0.0, "SELL": 0.0}
    # 라이브 Redis 카운터 재현
    adds_count = 0
    last_add_bar = -10**9

    close = df["close"].values.astype(float)
    rsi = df["rsi_14"].values.astype(float)
    bbu = df["bb_upper"].values.astype(float)
    bbl = df["bb_lower"].values.astype(float)
    adx = df["adx_14"].values.astype(float)
    atr = df["atr_14"].values.astype(float)
    atr_avg = pd.Series(atr).rolling(p.atr_lookback, min_periods=2).mean().values

    def close_position(i, price, reason):
        nonlocal equity, pos, adds_count
        notional = pos.qty * price
        fee = notional * p.fee_rate
        pnl = (price - pos.entry_price) * pos.qty if pos.side == "LONG" \
            else (pos.entry_price - price) * pos.qty
        equity += pnl - fee
        trades.append({"side": pos.side, "entry_idx": pos.entry_idx, "exit_idx": i,
                       "entry_price": pos.entry_price, "exit_price": price,
                       "qty": pos.qty, "duration": i - pos.entry_idx,
                       "pnl": pnl - fee, "reason": reason, "adds": pos.adds,
                       "entry_regime": pos.entry_regime})
        pos = Position()

    def open_position(i, price, side, regime):
        nonlocal equity, pos, adds_count, last_add_bar
        notional = equity * p.entry_portion
        qty = notional / price
        equity -= notional * p.fee_rate
        pos = Position(side=side, entry_price=price, qty=qty, entry_idx=i, entry_regime=regime)
        last_entry_bar["BUY" if side == "LONG" else "SELL"] = i
        last_entry_price["BUY" if side == "LONG" else "SELL"] = price
        if side == "LONG":          # 라이브: 신규 LONG 진입 시 카운터 리셋
            adds_count = 0
            last_add_bar = -10**9

    max_adds_seen = 0
    max_qty_mult = 1.0

    for i in range(len(df)):
        c = close[i]
        if np.isnan(bbu[i]) or np.isnan(rsi[i]) or np.isnan(bbl[i]):
            equity_curve.append(equity)
            continue
        regime = classify_regime(adx[i], atr[i], atr_avg[i], p)

        if pos.side != "FLAT":
            ret = (c - pos.entry_price) / pos.entry_price if pos.side == "LONG" \
                else (pos.entry_price - c) / pos.entry_price
            pos.peak_roi = max(pos.peak_roi, ret)
            exit_reason = None
            if ret <= p.soft_sl:
                exit_reason = "SOFT_SL"
            elif ret <= p.hard_sl:
                exit_reason = "HARD_SL"
            if exit_reason:
                close_position(i, c, exit_reason)
                equity_curve.append(equity)
                continue

        sig = trend_sig[i]
        if sig == "HOLD":
            equity_curve.append(equity)
            continue

        if pos.side == "FLAT":
            if regime == "FLAT_HOLD":
                equity_curve.append(equity)
                continue
            open_position(i, c, "LONG" if sig == "BUY" else "SHORT", regime)
        elif pos.side == "SHORT" and sig == "BUY":
            close_position(i, c, "REVERSE_EXIT")
        elif pos.side == "LONG" and sig == "SELL":
            close_position(i, c, "REVERSE_EXIT")
        elif pos.side == "LONG" and sig == "BUY":
            # ── 라이브 불타기 재현 ──
            ret = (c - pos.entry_price) / pos.entry_price
            # Redis TTL 재현: 마지막 add 후 60분 지나면 카운터 소멸
            if i - last_add_bar >= 60:
                adds_count = 0
            # 쿨다운 가드(라이브): 5분 내 동일방향 체결 있으면 '하락 시에만' 통과
            within_cd = (i - last_entry_bar["BUY"]) < p.cooldown_min
            buf = p.price_buffer_trend if regime == "TREND" else p.price_buffer_chop
            price_ok = (c <= last_entry_price["BUY"] * (1 - float(buf))) if within_cd else True
            pmax = p.pyramid_max if regime == "TREND" else 0
            if ret >= p.pyramid_profit and adds_count < pmax and price_ok and regime == "TREND":
                add_qty = pos.qty * p.pyramid_ratio
                equity -= add_qty * c * p.fee_rate
                new_qty = pos.qty + add_qty
                pos.entry_price = (pos.entry_price * pos.qty + c * add_qty) / new_qty
                pos.qty = new_qty
                pos.adds += 1
                adds_count += 1
                last_add_bar = i
                last_entry_bar["BUY"] = i
                last_entry_price["BUY"] = c
                max_adds_seen = max(max_adds_seen, pos.adds)

        if pos.side == "LONG" and pos.qty > 0:
            base_qty = (equity * p.entry_portion / pos.entry_price) if pos.entry_price else 1.0
        equity_curve.append(equity)

    if pos.side != "FLAT":
        close_position(len(df) - 1, close[-1], "EOD")
    return trades, np.array(equity_curve), equity, max_adds_seen


# ──────────────────────────────────────────────────────────────────────────
# [B] EMA 워밍업 윈도 신호 불일치 측정
# ──────────────────────────────────────────────────────────────────────────
def ema_window_mismatch(df: pd.DataFrame, window_bars: int,
                        ema_fast=30, ema_slow=60, tf_min=240) -> tuple:
    d = df.copy()
    d["dt"] = pd.to_datetime(d["timestamp_ms"], unit="ms", utc=True)
    htf = (d.set_index("dt").resample(f"{tf_min}min")
           .agg({"close": "last"}).dropna())
    closes = htf["close"].values.astype(float)
    full_f = htf["close"].ewm(span=ema_fast, adjust=False).mean().values
    full_s = htf["close"].ewm(span=ema_slow, adjust=False).mean().values

    start = ema_slow + 2           # 라이브 _min_htf_bars 워밍업과 동일 시점부터
    n = len(closes)
    mismatch = 0
    total = 0
    for i in range(start, n):
        lo = max(0, i - window_bars + 1)
        w = pd.Series(closes[lo:i + 1])
        if len(w) < ema_slow + 2:
            continue
        wf = w.ewm(span=ema_fast, adjust=False).mean().iloc[-1]
        ws = w.ewm(span=ema_slow, adjust=False).mean().iloc[-1]
        live_sig = "BUY" if wf > ws else "SELL"
        full_sig = "BUY" if full_f[i] > full_s[i] else "SELL"
        total += 1
        if live_sig != full_sig:
            mismatch += 1
    return mismatch, total


def run_is_oos_split(df: pd.DataFrame, is_ratio: float = 0.65):
    """피라미딩 채택 여부 판단용 IS/OOS 분리 검증 (CLAUDE.md 검증 규칙 준수).

    A-1(포지션당 2회 제한 피라미딩) vs A-2(피라미딩 없음)를
    학습(IS)·검증(OOS) 구간에서 각각 비교한다. 둘 다에서 우위여야 채택.
    """
    cut = int(len(df) * is_ratio)
    segments = [("IS (학습)", df.iloc[:cut].reset_index(drop=True)),
                ("OOS(검증)", df.iloc[cut:].reset_index(drop=True))]
    print("\n" + "=" * 64)
    print("[C] 피라미딩 IS/OOS 분리 검증 — 채택 기준: 두 구간 모두 우위")
    print("=" * 64)
    for label, seg in segments:
        days = len(seg) / 1440
        p_sim = StrategyParams(trend_mode=True)
        t1, c1, f1 = run_backtest(seg, p_sim)
        m1 = metrics(t1, c1, 10_000, f1)
        p_off = StrategyParams(trend_mode=True, pyramid_max=0)
        t2, c2, f2 = run_backtest(seg, p_off)
        m2 = metrics(t2, c2, 10_000, f2)
        print(f"\n  ── {label} ({days:.0f}일) ──")
        print(f"    피라미딩 2회/포지션 : 수익 {m1['return_pct']:+7.2f}%  MDD {m1['mdd_pct']:7.2f}%  Sharpe {m1['sharpe']:5.2f}  PF {m1['profit_factor']:.2f}")
        print(f"    피라미딩 없음       : 수익 {m2['return_pct']:+7.2f}%  MDD {m2['mdd_pct']:7.2f}%  Sharpe {m2['sharpe']:5.2f}  PF {m2['profit_factor']:.2f}")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--split-only", action="store_true", help="IS/OOS 분리 검증만 실행")
    args = ap.parse_args()

    df = load_data()
    df = compute_all_features(df)

    if args.split_only:
        run_is_oos_split(df)
        return

    print("\n" + "=" * 64)
    print("[A] TREND 모드 — 피라미딩 구성별 비교 (동일 데이터·동일 신호)")
    print("=" * 64)

    # A-1: backtest_live가 모델링한 TREND (포지션당 2회 + 유리단가 가드)
    p_sim = StrategyParams(trend_mode=True)
    t1, c1, f1 = run_backtest(df, p_sim)
    m1 = metrics(t1, c1, 10_000, f1)

    # A-2: 피라미딩 제거 (portfolio_backtest 검증 구성과 동일)
    p_off = StrategyParams(trend_mode=True, pyramid_max=0)
    t2, c2, f2 = run_backtest(df, p_off)
    m2 = metrics(t2, c2, 10_000, f2)

    # A-3: 라이브 충실 재현 (1h 카운터 리셋 + 반대 부등호 가드 + 방화벽 없음)
    t3, c3, f3, max_adds = run_backtest_live_faithful(df, StrategyParams(trend_mode=True))
    m3 = metrics(t3, c3, 10_000, f3)

    def row(label, m, extra=""):
        print(f"  {label:<34} 수익률 {m['return_pct']:+8.2f}%  MDD {m['mdd_pct']:7.2f}%  "
              f"Sharpe {m['sharpe']:5.2f}  거래 {m['trades']:>3}회 {extra}")

    row("A-1 시뮬 피라미딩(2회/포지션)", m1)
    row("A-2 피라미딩 없음(검증 구성)", m2)
    row("A-3 라이브 충실 재현(1h리셋)", m3, f"| 단일 포지션 최대 add {max_adds}회")

    adds_total_1 = sum(t["adds"] for t in t1)
    adds_total_3 = sum(t["adds"] for t in t3)
    print(f"\n  · A-1 총 추가매수 {adds_total_1}회 vs A-3(라이브) {adds_total_3}회")

    print("\n" + "=" * 64)
    print("[B] EMA 워밍업 윈도 — 4h봉 신호 불일치율 (전체이력 EMA 기준)")
    print("=" * 64)
    for wb in (96, 260):
        mm, tot = ema_window_mismatch(df, wb)
        days = wb * 4 / 24
        print(f"  윈도 {wb:>3}봉(≈{days:.0f}일): 불일치 {mm}/{tot} ({mm / tot * 100:.2f}%)")
    print("\n  (라이브 현재 = 96봉 윈도. 불일치 봉에서는 백테스트와 다른 포지션을 잡음)")


if __name__ == "__main__":
    main()
