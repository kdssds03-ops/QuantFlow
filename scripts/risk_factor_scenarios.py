"""
scripts/risk_factor_scenarios.py — RISK_FACTOR × 변동성타겟 상향 시나리오 (OOS 기준)

라이브 TREND 엔진을 충실 재현해 사이징 손잡이별 기대치를 산출한다:
  · 신호     : 4h EMA(30/60) 교차, 직전 완성봉 기준(스톱앤리버스, 항상 롱/숏)
  · 사이징   : 진입 시 equity × RISK_FACTOR × vol_scale (포지션 보유 중 리밸런싱 없음 — 라이브 동일)
  · vol_scale: clip(anchor/realized, 0.5, 2.0) — tasks.py::_vol_target_scale 동일 메커니즘
               (realized = 최근 30개 4h봉 수익률 std 연율화, anchor = 반감기 90일 EMA, 1봉 지연)
  · 청산     : 추세전환 역시그널 / 재난 소프트SL -12% (TREND 모드 라이브 동일)
  · 비용     : 시장가 수수료 0.05% 편도 (펀딩비·슬리피지 미반영 — 한계는 출력 하단 참고)
  · 브레이커 : 일중 MTM 자본이 당일 시작 대비 -5% 도달 시 그날 신규 진입 차단
               (MAX_DAILY_LOSS_PCT=0.05 라이브 동작 — 보유 포지션 청산 가드는 유지)

검증 규약: IS/OOS 60/40 분리 (portfolio_backtest.py 관례), 신호·변동성 추정 전부 1봉 지연.
RISK_FACTOR 상한 0.50: vol_scale 최대 2.0과 곱해도 노셔널 ≤ 자본 100%라서
무차입 모델(청산위험 미모델링)이 유효한 범위까지만 제시한다.

사용: python scripts/risk_factor_scenarios.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from scripts.backtest_live import load_data   # noqa: E402

FEE          = 0.0005          # 시장가 편도 수수료
SOFT_SL      = -0.12           # TREND 재난 스톱 (라이브 STOP_LOSS_THRESHOLD)
EMA_FAST, EMA_SLOW = 30, 60
WARMUP_BARS  = 62              # 신호 발화 최소 4h봉 수 (라이브 _min_htf_bars)
BPY_4H       = 365 * 6         # 4h봉/년
MIN_PER_YEAR = 525_600
VOL_BARS     = 30              # settings.vol_realized_4h_bars
ANCHOR_HL_4H = 90 * 6          # 앵커 반감기 90일 → 4h봉 540개
SCALE_MIN, SCALE_MAX = 0.5, 2.0
DAILY_BREAKER = -0.05          # MAX_DAILY_LOSS_PCT


def build_maps(df: pd.DataFrame):
    """1분봉 인덱스에 매핑된 (신호 ±1/NaN, vol_scale) 배열 생성 — 전부 완성봉 1봉 지연."""
    d = df.copy()
    d["dt"] = pd.to_datetime(d["timestamp_ms"], unit="ms", utc=True)
    htf = d.set_index("dt")["close"].resample("240min").last().dropna()

    ef = htf.ewm(span=EMA_FAST, adjust=False).mean()
    es = htf.ewm(span=EMA_SLOW, adjust=False).mean()
    sig4 = pd.Series(np.where(ef > es, 1.0, -1.0), index=htf.index)
    sig4.iloc[:WARMUP_BARS] = np.nan      # 워밍업 미달 구간 신호 없음
    sig4 = sig4.shift(1)                  # 직전 완성봉 기준

    ret4 = htf.pct_change()
    realized = ret4.rolling(VOL_BARS).std() * np.sqrt(BPY_4H)
    anchor = realized.ewm(halflife=ANCHOR_HL_4H).mean()
    scale4 = (anchor / realized).clip(SCALE_MIN, SCALE_MAX).shift(1)

    sig_1m = sig4.reindex(d["dt"], method="ffill").values
    scale_1m = scale4.reindex(d["dt"], method="ffill").fillna(1.0).values  # 미성숙 구간 1.0 (라이브 동일)
    return sig_1m, scale_1m


def run_segment(df, sig, scale, rf, vol_on, start, end, initial=10_000.0):
    """[start, end) 1분봉 구간 시뮬. 신호/스케일은 전체 이력 맵 사용(라이브 연속 가동 재현)."""
    close = df["close"].values.astype(float)
    ts = df["timestamp_ms"].values
    day_kst = (ts + 9 * 3600 * 1000) // 86_400_000

    cash = initial
    side, qty, entry = 0, 0.0, 0.0
    round_trips = 0
    fees = 0.0
    sl_exits = 0
    mtm = np.empty(end - start)
    day_start_eq: dict = {}
    tripped_days: set = set()
    scales_used = []

    for k in range(end - start):
        i = start + k
        c = close[i]
        s = sig[i]

        if side != 0:
            ret = (c - entry) / entry * side
            flip = (not np.isnan(s)) and int(s) != side
            if ret <= SOFT_SL or flip:
                fee = c * qty * FEE
                cash += side * (c - entry) * qty - fee
                fees += fee
                round_trips += 1
                if ret <= SOFT_SL and not flip:
                    sl_exits += 1
                side, qty = 0, 0.0

        d = day_kst[i]
        eq_now = cash + (side * (c - entry) * qty if side else 0.0)
        if d not in day_start_eq:
            day_start_eq[d] = eq_now
        if eq_now / day_start_eq[d] - 1 <= DAILY_BREAKER:
            tripped_days.add(d)          # 그날 신규 진입 차단 (청산 가드는 위에서 계속)

        if side == 0 and not np.isnan(s) and d not in tripped_days:
            sc = scale[i] if vol_on else 1.0
            notional = cash * rf * sc
            qty = notional / c
            entry = c
            side = int(s)
            fee = notional * FEE
            cash -= fee
            fees += fee
            scales_used.append(sc)

        mtm[k] = cash + (side * (c - entry) * qty if side else 0.0)

    if side != 0:                        # 구간 말 평가 청산
        c = close[end - 1]
        fee = c * qty * FEE
        cash += side * (c - entry) * qty - fee
        fees += fee
        round_trips += 1
        mtm[-1] = cash

    yrs = (end - start) / MIN_PER_YEAR
    final = mtm[-1]
    cagr = (final / initial) ** (1 / yrs) - 1 if yrs > 0 else 0.0
    peak = np.maximum.accumulate(mtm)
    mdd = ((mtm - peak) / peak).min()
    r = np.diff(mtm) / mtm[:-1]
    sharpe = r.mean() / r.std() * np.sqrt(MIN_PER_YEAR) if r.std() > 0 else 0.0
    return {
        "cagr": cagr * 100, "mdd": mdd * 100, "sharpe": sharpe,
        "trades": round_trips, "fees": fees, "sl_exits": sl_exits,
        "breaker_days": len(tripped_days),
        "avg_scale": float(np.mean(scales_used)) if scales_used else 1.0,
    }


def main():
    df = load_data()
    sig, scale = build_maps(df)
    n = len(df)
    split = int(n * 0.6)
    is_days, oos_days = split / 1440, (n - split) / 1440
    print(f"[구간] IS {is_days:.0f}일 / OOS {oos_days:.0f}일 (60/40), 신호·스케일 1봉 지연, 수수료 {FEE*100:.2f}%/편도")

    grid = [0.10, 0.15, 0.20, 0.30, 0.40, 0.50]
    for vol_on in (True, False):
        title = "변동성타겟 ON (라이브 현행 메커니즘)" if vol_on else "변동성타겟 OFF (고정 사이징 비교용)"
        print("\n" + "=" * 100)
        print(f"[{title}]")
        print("=" * 100)
        print(f"{'RF':>5} | {'IS CAGR':>8} {'IS MDD':>8} {'IS Shp':>7} | "
              f"{'OOS CAGR':>8} {'OOS MDD':>8} {'OOS Shp':>7} | {'OOS브레이커':>9} {'OOS손절':>7} {'평균스케일':>9}")
        print("-" * 100)
        for rf in grid:
            mi = run_segment(df, sig, scale, rf, vol_on, 0, split)
            mo = run_segment(df, sig, scale, rf, vol_on, split, n)
            print(f"{rf:>5.2f} | {mi['cagr']:>+7.2f}% {mi['mdd']:>+7.2f}% {mi['sharpe']:>7.2f} | "
                  f"{mo['cagr']:>+7.2f}% {mo['mdd']:>+7.2f}% {mo['sharpe']:>7.2f} | "
                  f"{mo['breaker_days']:>7d}일 {mo['sl_exits']:>6d}회 {mo['avg_scale']:>8.2f}x")

    print("""
[모델 한계 — 기대치를 깎아서 볼 것]
  · 펀딩비 미반영: 항상 포지션 보유 전략이라 롱 구간 평균 펀딩(-)이 실적을 추가로 깎을 수 있음
  · 슬리피지 미반영(4h 저빈도라 영향 작음), 무차입 가정(RF≤0.5 × scale≤2.0 → 노셔널 ≤ 자본)
  · OOS 383일 = 표본 1개. 구간이 바뀌면 수치는 달라진다 — 비율(위험 대비)로만 해석할 것
""")


if __name__ == "__main__":
    main()
