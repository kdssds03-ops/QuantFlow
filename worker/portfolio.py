"""
worker.portfolio — 멀티심볼 포트폴리오 사이징 (리스크패리티 + 변동성타게팅 + 레버 k)

검증 근거: scripts/improve_research.py · scripts/backtest_live.py --portfolio
  4h EMA(30/60) 신호를 메이저4(BTC/ETH/SOL/BNB)에 분산 → 단일 BTC 대비 OOS Sharpe
  0.78→1.10, MDD는 레버 k로 통제. (8코인은 수익↑이나 DOGE/AVAX 의존·고슬리피지)

설계(라이브 안전판):
  - 가중치   : 변동성 역수(리스크패리티) — 변동성 큰 코인을 자동 저가중.
  - 레버     : 포트 변동성타겟. lev = clip(target_vol / 포트변동성, 0, max_lev).
  - 위험손잡이: gross = min(lev × k, MAX_GROSS).  ← MAX_GROSS가 청산 방지 하드캡.
  - 심볼별   : effective_risk_i = min(weight_i × gross, per_symbol_cap).
               (per_symbol_cap = 자본방화벽 × 0.95 — 자기거부 방지)
  - 진입 시 1회 샘플링(라이브 충실). 백테스트의 매봉 리밸런싱보다 보수적이라
    실측은 약간 열위일 수 있음 — 분산(가중치) 이득은 그대로 유효.

⚠️ 데이터 전제: 포트 심볼 전부의 1m 캔들이 DB에 ≥5일 쌓여 있어야 vol 추정이 됨.
   부족 시 이 함수는 0.0(=진입 보류)을 반환 → 워밍업 안전.
   (beat가 모든 심볼을 fetch + scripts/backfill_db.py로 초기 백필 필요)

순수 stdlib + pandas/numpy 의존 (DB/Redis는 콜백 주입) → 단위검증 가능.
"""
from __future__ import annotations

import logging
import math
from decimal import Decimal
from typing import Callable, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BPY_4H = 365 * 6  # 4h봉/년 = 2190 (연율화 상수)


def _resample_4h_returns(rows: Sequence[Tuple[int, float]]) -> pd.Series:
    """1m (ts_ms, close) 오름차순 → 4h 종가 수익률 시리즈. 룩어헤드 없음(과거 봉만)."""
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["ts", "close"])
    df = df.drop_duplicates("ts").sort_values("ts")
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    c4 = df.set_index("dt")["close"].resample("240min").last().dropna()
    return c4.pct_change().dropna()


def compute_sizing(
    returns_by_symbol: Dict[str, pd.Series],
    *,
    n_bars: int,
    target_vol: float,
    max_lev: float,
    k: float,
    max_gross: float,
    per_symbol_cap: float,
) -> Dict[str, float]:
    """심볼별 effective_risk(가용마진 대비 진입 노셔널 비율)를 산출.

    반환: {symbol: effective_risk_fraction}. 데이터 부족 심볼은 0.0.
    합( effective_risk ) ≈ gross ≤ max_gross 로 총 노출 제한.
    """
    # 1) 심볼별 실현변동성(연율) — 최근 n_bars개 4h 수익률
    vols: Dict[str, float] = {}
    for sym, ret in returns_by_symbol.items():
        r = ret.dropna()
        if len(r) < max(n_bars // 2, 5):     # 최소 표본 미달 → 제외
            continue
        v = float(r.tail(n_bars).std()) * math.sqrt(BPY_4H)
        if v > 0 and not math.isnan(v):
            vols[sym] = v
    if not vols:
        return {s: 0.0 for s in returns_by_symbol}

    # 2) 리스크패리티 가중 (변동성 역수 정규화)
    inv = {s: 1.0 / v for s, v in vols.items()}
    inv_sum = sum(inv.values())
    weights = {s: inv[s] / inv_sum for s in inv}

    # 3) 포트폴리오 변동성 (공통 구간 가중합 수익률의 표준편차)
    common = None
    for s in weights:
        idx = returns_by_symbol[s].dropna().tail(n_bars).index
        common = idx if common is None else common.intersection(idx)
    port_vol = None
    if common is not None and len(common) >= max(n_bars // 2, 5):
        port_ret = sum(returns_by_symbol[s].reindex(common).fillna(0.0) * weights[s] for s in weights)
        sd = float(port_ret.std())
        if sd > 0 and not math.isnan(sd):
            port_vol = sd * math.sqrt(BPY_4H)
    if port_vol is None:
        # 공통구간 부족 → 분산 무가정(가중평균 변동성)으로 보수적 추정
        port_vol = sum(weights[s] * vols[s] for s in weights)

    # 4) 변동성타겟 레버 → gross 하드캡 → 심볼별 클램프
    lev = min(max(target_vol / port_vol, 0.0), max_lev) if port_vol > 0 else 0.0
    gross = min(lev * k, max_gross)
    out: Dict[str, float] = {s: 0.0 for s in returns_by_symbol}
    for s in weights:
        out[s] = min(weights[s] * gross, per_symbol_cap)
    logger.info(
        "📦 [PORTFOLIO] 포트변동성 %.1f%% → lev %.2f × k → gross %.2f | 가중치 %s",
        port_vol * 100, lev, gross,
        {s: round(w, 3) for s, w in weights.items()},
    )
    return out


def portfolio_effective_risk(
    symbol: str,
    *,
    symbols: List[str],
    fetch_1m_closes: Callable[[str, int], Sequence[Tuple[int, float]]],
    n_bars: int = 30,
    target_vol: float = 0.15,
    max_lev: float = 3.0,
    k: float = 1.0,
    max_gross: float = 1.5,
    per_symbol_cap: float = 0.57,
) -> Decimal:
    """주어진 symbol의 진입 사이징 비율(Decimal)을 반환. 오류/부족 시 0.0(진입 보류).

    fetch_1m_closes(sym, need_1m) → [(ts_ms, close), ...] 오름차순 (DB 콜백 주입).
    """
    try:
        if symbol not in symbols:
            symbols = list(symbols) + [symbol]
        need_1m = 240 * (n_bars + 3)
        returns_by_symbol: Dict[str, pd.Series] = {}
        for s in symbols:
            rows = fetch_1m_closes(s, need_1m)
            returns_by_symbol[s] = _resample_4h_returns(rows)
        sizing = compute_sizing(
            returns_by_symbol,
            n_bars=n_bars, target_vol=target_vol, max_lev=max_lev,
            k=k, max_gross=max_gross, per_symbol_cap=per_symbol_cap,
        )
        val = float(sizing.get(symbol, 0.0))
        if not (val > 0) or math.isnan(val):
            logger.info("⏸️ [PORTFOLIO] %s 사이징 0(데이터 부족/워밍업) → 진입 보류", symbol)
            return Decimal("0")
        return Decimal(str(round(val, 6)))
    except Exception as exc:  # 어떤 예외든 안전하게 진입 보류
        logger.warning("⚠️ [PORTFOLIO] %s 사이징 산출 실패 → 0(진입 보류): %s", symbol, exc)
        return Decimal("0")
