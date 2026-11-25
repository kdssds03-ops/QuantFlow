"""주식 나침반 — 리스크·비중 관리 계산 (순수 함수 + 얇은 수집 계층).

가격을 예측하지 않는다. "산다면 얼마나, 어디서 끊으면 얼마 잃는지"와
"지금 보유한 것들이 얼마나 쏠려 있고 하루에 얼마나 흔들리는지"만 계산한다.
"""
from datetime import date, timedelta
import json
import math
import os
from pathlib import Path
import statistics as st

from scripts import kr_compass as k

PORTFOLIO_FILE = Path(__file__).resolve().parents[1] / "data/kr_portfolio.json"  # 개인정보 — gitignore
LIMIT_DOWN = 0.30          # KRX 일일 가격제한폭(주식·일반 ETF). 레버리지 ETF는 더 큼
MIN_COMMON_DAYS = 60       # 상관·변동성 계산 최소 공통 거래일
LOOKBACK = 250             # 포트폴리오 통계 창(거래일)
MAX_HOLDINGS = 50


# ── 수량 계산 ────────────────────────────────────────
def size_position(account, risk_pct, entry, stop, max_pos_pct=20.0, cash=None):
    """입력 조건에서 살 수 있는 최대 수량.

    shares = floor(계좌 × 위험% ÷ (진입가 − 손절가)), 이후 최대 비중·가용 현금으로 상한.
    """
    if account <= 0 or entry <= 0:
        return {"state": "error", "message": "계좌 금액과 진입가는 0보다 커야 합니다."}
    if stop is None or stop >= entry:
        return {"state": "error", "message": "손절가는 진입가보다 낮아야 합니다."}
    per_share = entry - stop
    budget = account * risk_pct / 100
    caps = {"위험 한도": math.floor(budget / per_share),
            "최대 비중": math.floor(account * max_pos_pct / 100 / entry)}
    if cash is not None:
        caps["가용 현금"] = math.floor(cash / entry)
    binding = min(caps, key=caps.get)
    shares = max(caps[binding], 0)
    value = shares * entry
    out = {
        "state": "ok" if shares > 0 else "zero",
        "shares": shares, "binding": binding, "caps": caps,
        "entry": entry, "stop": stop, "risk_per_share": per_share,
        "stop_pct": -per_share / entry * 100,
        "position_value": value, "position_pct": value / account * 100,
        "risk_budget": budget,
        "loss_at_stop": shares * per_share, "loss_at_stop_pct": shares * per_share / account * 100,
        "loss_limit_down": value * LIMIT_DOWN, "loss_limit_down_pct": value * LIMIT_DOWN / account * 100,
    }
    if shares == 0:
        out["message"] = f"{binding} 기준으로 1주도 살 수 없습니다. 위험%를 높이거나 손절폭을 줄이거나 금액을 늘려야 합니다."
    return out


# ── 포트폴리오 ───────────────────────────────────────
def returns_by_date(bars):
    """{날짜: 일간수익률}. 거래정지로 빠진 날은 다음 거래일 수익률에 합산된다."""
    return {b[0]: b[4] / p[4] - 1 for p, b in zip(bars, bars[1:])}


def portfolio_stats(holdings, cash=0.0):
    """holdings: [{code, name, shares, stop?, avg?, industry?, bars}] (bars = 완료 일봉).

    가중치는 현재 평가금액(현금 포함 분모). 수익률은 날짜 교집합으로 정렬한다.
    """
    rows, total = [], cash
    for h in holdings:
        bars = h.get("bars") or []
        price = bars[-1][4] if bars else None
        value = price * h["shares"] if price else 0.0
        total += value
        rows.append({**{k_: h.get(k_) for k_ in ("code", "name", "shares", "stop", "avg", "industry", "now")},
                     "price": price, "value": value, "bars": bars,
                     "price_date": f"{bars[-1][0]}" if bars else None})
    if total <= 0:
        return {"state": "empty", "message": "평가금액이 없습니다."}

    open_risk, undefined = 0.0, []
    for r in rows:
        r["weight"] = r["value"] / total * 100
        r["alerts"] = alerts_for(r)
        if r["price"] is None:
            r["risk"] = None
        elif r["stop"]:
            r["risk"] = max(r["price"] - r["stop"], 0) * r["shares"]
            open_risk += r["risk"]
        else:
            r["risk"] = None
            undefined.append(r["name"] or r["code"])
        if r["avg"] and r["price"]:
            r["pnl_pct"] = (r["price"] / r["avg"] - 1) * 100

    industries = {}
    for r in rows:
        if r["industry"]:
            g = industries.setdefault(r["industry"], {"weight": 0.0, "names": []})
            g["weight"] += r["weight"]
            g["names"].append(r["name"] or r["code"])

    stats = series_stats(rows, total)
    for r in rows:
        r.pop("bars")
    return {
        "state": "ok", "total": total, "cash": cash, "cash_pct": cash / total * 100,
        "holdings": sorted(rows, key=lambda r: -r["value"]),
        "open_risk": open_risk, "open_risk_pct": open_risk / total * 100, "risk_undefined": undefined,
        "limit_down_loss_pct": sum(r["value"] for r in rows) * LIMIT_DOWN / total * 100,
        "max_weight": max((r["weight"] for r in rows), default=0),
        "industries": sorted(({"code": c, **g} for c, g in industries.items() if len(g["names"]) > 1),
                             key=lambda g: -g["weight"]),
        **stats,
    }


def series_stats(rows, total):
    """현재 비중으로 구성한 포트폴리오 일간수익률 시계열 → 변동성·최악일·하위 5%·상관."""
    live = [r for r in rows if r["bars"] and r["value"] > 0]
    if not live:
        return {"stats": None, "stats_note": "보유 종목 시세 없음"}
    rets = {r["code"]: returns_by_date(r["bars"][-(LOOKBACK + 1):]) for r in live}
    common = sorted(set.intersection(*(set(v) for v in rets.values())))
    note = f"공통 거래일 {len(common)}일"
    if common:
        note += f" ({common[0]} ~ {common[-1]})"
    if len(common) < MIN_COMMON_DAYS:
        short = min(live, key=lambda r: len(rets[r["code"]]))
        return {"stats": None,
                "stats_note": f"{note} — {MIN_COMMON_DAYS}일 미만이라 계산 생략 (가장 짧은 종목: {short['name'] or short['code']})"}
    w = {r["code"]: r["value"] / total for r in live}  # 현금은 수익률 0
    port = [sum(w[c] * rets[c][d] for c in w) for d in common]
    srt = sorted(port)
    worst_i = port.index(srt[0])
    codes = [r["code"] for r in live]
    corr = {a: {b: correlation([rets[a][d] for d in common], [rets[b][d] for d in common]) for b in codes}
            for a in codes}
    return {
        "stats": {
            "days": len(common), "vol_daily": st.pstdev(port) * 100,
            "vol_annual": st.pstdev(port) * math.sqrt(252) * 100,
            "worst_day": srt[0] * 100, "worst_date": f"{common[worst_i]}",
            "p5_day": srt[max(int(len(srt) * 0.05) - 1, 0)] * 100,
            "corr": corr, "names": {r["code"]: r["name"] or r["code"] for r in live},
        },
        "stats_note": note,
    }


def correlation(xs, ys):
    mx, my = st.mean(xs), st.mean(ys)
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (sx * sy) if sx and sy else None


def alerts_for(r):
    """화면을 열 때 계산되는 경보(푸시 아님)."""
    out, bars, price = [], r["bars"], r["price"]
    if price is None:
        return ["시세 없음"]
    if r["stop"] and price <= r["stop"]:
        out.append(f"손절선 이탈 (종가 {price:,.0f} ≤ 손절 {r['stop']:,.0f})")
    elif r["stop"] and r.get("now") and r["now"] <= r["stop"]:
        out.append(f"장중 손절선 아래 (현재가 {r['now']:,.0f} ≤ 손절 {r['stop']:,.0f}, 종가 미확정)")
    a14, a60 = k.atr(bars, 14), k.atr(bars, 60)
    if a14 and a60 and a14 / a60 >= 1.5:
        out.append(f"변동성 급증 (ATR14가 ATR60의 {a14 / a60:.1f}배)")
    hi20 = max((b[2] for b in bars[-20:]), default=None)
    if hi20 and price / hi20 - 1 <= -0.15:
        out.append(f"20일 고점 대비 {(price / hi20 - 1) * 100:.0f}%")
    return out


# ── 저장 (개인 보유 내역 — 로컬 파일, 원자적 쓰기) ────
def validate_portfolio(p):
    if not isinstance(p, dict):
        raise ValueError("형식 오류")
    cash = float(p.get("cash") or 0)
    if not 0 <= cash < 1e13:
        raise ValueError("현금 값이 올바르지 않습니다.")
    items = p.get("holdings") or []
    if not isinstance(items, list) or len(items) > MAX_HOLDINGS:
        raise ValueError(f"보유 종목은 최대 {MAX_HOLDINGS}개입니다.")
    out, seen = [], set()
    for h in items:
        code = str(h.get("code", "")).upper()
        if not k.CODE_RE.match(code):
            raise ValueError(f"종목코드 오류: {code}")
        if code in seen:
            raise ValueError(f"{code}이(가) 두 번 들어 있습니다. 한 줄로 합쳐 주세요.")
        seen.add(code)
        try:
            shares = float(h.get("shares") or 0)
        except (TypeError, ValueError):
            raise ValueError(f"{code} 수량은 숫자여야 합니다.")
        if shares != int(shares):
            raise ValueError(f"{code} 수량은 정수여야 합니다.")
        shares = int(shares)
        if not 0 < shares < 1e9:
            raise ValueError(f"{code} 수량은 1 이상이어야 합니다.")
        clean = {"code": code, "name": str(h.get("name") or code)[:40], "shares": shares}
        for f in ("stop", "avg"):
            v = h.get(f)
            if v not in (None, ""):
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    raise ValueError(f"{code} {f} 값은 숫자여야 합니다.")
                if not 0 < v < 1e9:
                    raise ValueError(f"{code} {f} 값 오류")
                clean[f] = v
        out.append(clean)
    return {"cash": cash, "holdings": out}


def load_portfolio():
    try:
        return json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"cash": 0, "holdings": []}


def save_portfolio(p):
    clean = validate_portfolio(p)
    PORTFOLIO_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PORTFOLIO_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(clean, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, PORTFOLIO_FILE)
    return clean


# ── 수집 ────────────────────────────────────────────
def closed_bars(code, with_live=False):
    """완료 일봉만 (장중이면 오늘 봉 제외). with_live=True면 (bars, 장중 현재가 또는 None)."""
    bars = k.fetch_daily(code)
    now = None
    try:
        basic = k._get(f"https://m.stock.naver.com/api/stock/{code}/basic")
        live = basic.get("marketStatus") == "OPEN"
        now = k.num(basic.get("closePrice")) if live else None
    except Exception:
        live = True  # 모르면 보수적으로 오늘 봉 제외
    bars = bars[:-1] if live and bars and bars[-1][0] == date.today() else bars
    return (bars, now) if with_live else bars


def stock_meta(code):
    try:
        integ = k._get(f"https://m.stock.naver.com/api/stock/{code}/integration")
        return integ.get("stockName"), integ.get("industryCode")
    except Exception:
        return None, None


def recent_disclosures(code, days=7):
    try:
        rows = k._get(f"https://m.stock.naver.com/api/stock/{code}/disclosure?pageSize=10")
    except Exception:
        return []
    since = (date.today() - timedelta(days=days)).isoformat()
    return [{"title": r.get("title"), "date": (r.get("datetime") or "")[:10]}
            for r in rows if (r.get("datetime") or "") >= since][:5]


def analyze_saved():
    p = load_portfolio()
    holdings, errors = [], []
    for h in p["holdings"]:
        try:
            bars, now = closed_bars(h["code"], with_live=True)
        except Exception as e:
            bars, now = [], None
            errors.append(f"{h['code']} 시세 조회 실패: {e}")
        name, industry = stock_meta(h["code"])
        holdings.append({**h, "name": name or h.get("name"), "industry": industry, "bars": bars, "now": now})
    result = portfolio_stats(holdings, p["cash"])
    if result["state"] == "ok":
        dates = sorted({r["price_date"] for r in result["holdings"] if r.get("price_date")})
        result["price_basis"] = f"평가 기준: KRX 정규장 종가 {', '.join(dates)}"
        live = [r for r in result["holdings"] if r.get("now")]
        if live:
            result["price_basis"] += " · 장중이라 오늘 시세는 손절선 이탈 점검에만 반영"
    if result["state"] == "ok":
        for r in result["holdings"]:
            r["disclosures"] = recent_disclosures(r["code"])
    result["errors"] = errors
    return result


def size_for(code, account, risk_pct, max_pos_pct=20.0, cash=None, entry=None, stop=None, atr_mult=2.0):
    if not k.CODE_RE.match(code):
        raise ValueError("종목코드 오류")
    bars = closed_bars(code)
    if not bars:
        raise ValueError("시세를 가져오지 못했습니다.")
    name, _ = stock_meta(code)
    last = bars[-1][4]
    a = k.atr(bars)
    entry = entry or last
    default_stop = entry - atr_mult * a if a else None
    res = size_position(account, risk_pct, entry, stop or default_stop, max_pos_pct, cash)
    res.update({"code": code, "name": name or code, "last_close": last, "last_date": f"{bars[-1][0]}",
                "atr": a, "atr_pct": a / last * 100 if a else None, "stop_source": "직접 입력" if stop else f"{atr_mult:g}×ATR14"})
    return res
