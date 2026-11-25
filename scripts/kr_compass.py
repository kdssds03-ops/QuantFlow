"""한국 주식 스윙 진단 로컬 웹앱 (주식 나침반).

종목 하나를 넣으면 스윙(수일~수주) 관점의 조건을 신호등으로 보여준다.
매수/매도 추천이 아니라 판단 근거를 모아 보여주는 도구이며, 규칙은 백테스트로 검증되지 않았다.

실행(프로젝트 루트):  .venv/bin/python -m scripts.kr_compass  →  http://127.0.0.1:8765
데이터: 네이버 금융 공개 엔드포인트(비공식). 지표는 모두 KRX 정규장 일봉(siseJson) 기준.
"""
import argparse
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
import time

import httpx

PAGE = Path(__file__).with_name("kr_compass.html")
CODE_RE = re.compile(r"^[0-9A-Z]{6}$")  # 신규 상장은 0080G0 같은 영숫자 코드
CACHE_SECONDS = 60
_cache = {}
_client = httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=10)


# ── 파싱 ──────────────────────────────────────────────
def num(text):
    """'196,400' '+1,104,534' '8.81배' '45.95%' → float, 실패 시 None. '5조 3,553억' 같은 값은 None."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text)
    s = str(text).replace(",", "").strip()
    m = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)\s*(배|%|원)?", s)
    return float(m.group(1)) if m else None


# ── 수집 (네트워크) ──────────────────────────────────
def _get(url, is_json=True):
    hit = _cache.get(url)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    r = _client.get(url)
    r.raise_for_status()
    data = r.json() if is_json else r.text
    _cache[url] = (time.time(), data)
    return data


def fetch_daily(code, days=420):
    """KRX 정규장 일봉. [(date, o, h, l, c, v), ...] 오래된 순."""
    end = date.today()
    start = end - timedelta(days=days)
    url = (f"https://api.finance.naver.com/siseJson.naver?symbol={code}&requestType=1"
           f"&startTime={start:%Y%m%d}&endTime={end:%Y%m%d}&timeframe=day")
    rows = json.loads(_get(url, is_json=False).replace("'", '"'))
    return parse_daily(rows)


def parse_daily(rows):
    """siseJson 행 → 일봉. 거래정지일(시가·거래량 0) 행은 제외."""
    out = []
    for r in rows[1:]:
        try:
            bar = (datetime.strptime(r[0], "%Y%m%d").date(), *map(float, r[1:6]))
        except (ValueError, TypeError, IndexError):
            continue
        if bar[1] > 0 and bar[5] > 0:
            out.append(bar)
    return out


def search(query):
    q = httpx.QueryParams({"q": query, "target": "stock"})
    items = _get(f"https://ac.stock.naver.com/ac?{q}").get("items", [])
    return [{"code": i["code"], "name": i["name"], "market": i.get("typeName", "")}
            for i in items if i.get("nationCode") == "KOR" and CODE_RE.match(i.get("code", ""))][:10]


# ── 지표 (순수 함수) ─────────────────────────────────
def sma(values, n):
    return sum(values[-n:]) / n if len(values) >= n else None


def rsi(closes, n=14):
    """Wilder RSI."""
    if len(closes) < n + 1:
        return None
    gains = [max(closes[i] - closes[i - 1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i - 1] - closes[i], 0) for i in range(1, len(closes))]
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for g, l in zip(gains[n:], losses[n:]):
        ag, al = (ag * (n - 1) + g) / n, (al * (n - 1) + l) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def atr(bars, n=14):
    """Wilder ATR. bars = [(d, o, h, l, c, v)]."""
    if len(bars) < n + 1:
        return None
    trs = [max(b[2] - b[3], abs(b[2] - p[4]), abs(b[3] - p[4])) for p, b in zip(bars, bars[1:])]
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a


GREEN, YELLOW, RED, GREY = "green", "yellow", "red", "grey"

# scripts/kr_compass_backtest.py 결과 (2026-11-18, 시총 상위 250종목, 2012~2026, IS ~2019 / OOS 2020~).
# 값 = 신호 후 10거래일, 같은 날 전체 평균 대비 '초록 − 빨강' 초과수익 격차. 왕복 비용 ~0.4%.
VALIDATION = {
    "trend": "백테스트: 효과 없음 (초록−빨강 IS +0.06% / OOS +0.13%, 유의하지 않음)",
    "heat": "백테스트: 역방향 — 빨강(과열·과매도)이 오히려 더 올랐음 (IS −0.58% / OOS −0.81%). "
            "과열 구간이 평균보다 덜 오르는 현상은 이 표본에서 관찰되지 않음(사후 분석, 유의하지 않음). "
            "생존편향이 이 방향으로 결과를 부풀릴 수 있어 '추격해도 된다'는 뜻은 아님",
    "flow": "백테스트(상위 60종목, 2015~): 효과 없음 (IS −0.39% / OOS −0.17%, 유의하지 않음)",
    "volume": "백테스트: 효과 없음 (IS −0.29% / OOS −0.37%, 부호 불안정)",
    "position": "백테스트: 효과 없음 (IS −0.01% / OOS +0.17%, 유의하지 않음)",
}


def light(key, title, color, value, rule):
    return {"key": key, "title": title, "color": color, "value": value, "rule": rule,
            "validation": VALIDATION.get(key, "")}


def check_trend(closes):
    rule = "충족: 종가 > MA20 > MA60 · 미충족: 종가 < MA20 < MA60 · 그 외 혼조"
    if len(closes) < 61:
        return light("trend", "추세", GREY, "자료 부족 (일봉 61개 미만)", rule)
    c, m20, m60 = closes[-1], sma(closes, 20), sma(closes, 60)
    m20_prev = sma(closes[:-5], 20)
    slope = (m20 / m20_prev - 1) * 100
    val = f"종가 {c:,.0f} · MA20 {m20:,.0f} · MA60 {m60:,.0f} · MA20 5일 기울기 {slope:+.1f}%"
    if c > m20 > m60:
        return light("trend", "추세", GREEN, val, rule)
    if c < m20 < m60:
        return light("trend", "추세", RED, val, rule)
    return light("trend", "추세", YELLOW, val, rule)


def check_heat(closes):
    rule = "충족: RSI 40~65 & MA20 이격 +8% 이내 · 과열: RSI>70 또는 이격 > +12% · 약세: RSI<30"
    if len(closes) < 21:
        return light("heat", "과열 아님 (추격 위험)", GREY, "자료 부족", rule)
    r, gap = rsi(closes), (closes[-1] / sma(closes, 20) - 1) * 100
    val = f"RSI14 {r:.0f} · MA20 이격 {gap:+.1f}%"
    if r < 30:
        return light("heat", "과열 아님 (추격 위험)", RED, val + " · 과매도(약세 지속 위험)", rule)
    if r > 70 or gap > 12:
        val += " · 과열"
        return light("heat", "과열 아님 (추격 위험)", RED, val, rule)
    if 40 <= r <= 65 and gap <= 8:
        return light("heat", "과열 아님 (추격 위험)", GREEN, val, rule)
    return light("heat", "과열 아님 (추격 위험)", YELLOW, val, rule)


def check_flow(trend_rows, price):
    rule = "최근 5거래일 외국인+기관 순매수 합(금액 환산) · 충족: 순매수 & 5일 중 3일 이상 · 미충족: 순매도 & 3일 이상"
    rows = [r for r in trend_rows[:5]
            if num(r.get("foreignerPureBuyQuant")) is not None and num(r.get("organPureBuyQuant")) is not None]
    if len(rows) < 5 or not price:
        return light("flow", "수급", GREY, "자료 부족", rule)
    daily = [num(r["foreignerPureBuyQuant"]) + num(r["organPureBuyQuant"]) for r in rows]
    f5 = sum(num(r["foreignerPureBuyQuant"]) for r in rows) * price / 1e8
    o5 = sum(num(r["organPureBuyQuant"]) for r in rows) * price / 1e8
    buy_days = sum(d > 0 for d in daily)
    val = f"5일 외국인 {f5:+,.0f}억 · 기관 {o5:+,.0f}억 · 합산 순매수일 {buy_days}/5"
    if f5 + o5 > 0 and buy_days >= 3:
        return light("flow", "수급", GREEN, val, rule)
    if f5 + o5 < 0 and buy_days <= 2:
        return light("flow", "수급", RED, val, rule)
    return light("flow", "수급", YELLOW, val, rule)


def check_volume(bars):
    rule = "최근 완료 봉 거래량 / 직전 20일 평균 · 상승일 1.5배↑ 충족 · 하락일 1.5배↑ 미충족"
    if len(bars) < 22:
        return light("volume", "거래량", GREY, "자료 부족", rule)
    last, prev = bars[-1], bars[-2]
    if len(bars) >= 42:  # 가격은 수정주가인데 거래량은 미수정 → 분할 직후엔 배수가 왜곡됨
        before = sorted(b[5] for b in bars[-42:-21])[10]
        recent = sorted(b[5] for b in bars[-21:])[10]
        if before > 0 and not 1 / 8 < recent / before < 8:
            return light("volume", "거래량", GREY, "거래량 수준 급변(액면분할 등) — 배수 비교 불가", rule)
    avg = sum(b[5] for b in bars[-21:-1]) / 20
    ratio = last[5] / avg if avg else 0
    up = last[4] >= prev[4]
    val = f"{last[0]:%m/%d} 거래량 {ratio:.1f}배 ({'상승' if up else '하락'}일)"
    if ratio >= 1.5:
        return light("volume", "거래량", GREEN if up else RED, val, rule)
    return light("volume", "거래량", YELLOW, val, rule)


def check_position(bars):
    rule = "KRX 정규장 1년 고저 범위 내 위치 · 충족: 50~90% · 미충족: 20% 미만(하락 추세 구간)"
    year = [b for b in bars if b[0] >= bars[-1][0] - timedelta(days=365)] if bars else []
    if len(year) < 120:
        return light("position", "1년 범위 위치", GREY, "자료 부족 (상장 1년 미만 등)", rule)
    hi, lo, c = max(b[2] for b in year), min(b[3] for b in year), bars[-1][4]
    pos = (c - lo) / (hi - lo) * 100 if hi > lo else 50
    val = f"{pos:.0f}% (저 {lo:,.0f} ~ 고 {hi:,.0f}) · 고점 대비 {(c / hi - 1) * 100:+.1f}%"
    if 50 <= pos <= 90:
        return light("position", "1년 범위 위치", GREEN, val, rule)
    if pos < 20:
        return light("position", "1년 범위 위치", RED, val, rule)
    return light("position", "1년 범위 위치", YELLOW, val, rule)


def risk_box(bars):
    """판단 보조: ATR 기반 변동성과 참고 손절 거리. 추천이 아니라 계산값."""
    a = atr(bars)
    if a is None:
        return None
    c = bars[-1][4]
    return {"atr": a, "atr_pct": a / c * 100, "stop_2atr": c - 2 * a, "stop_pct": -2 * a / c * 100}


def diagnose_from(bars, trend_rows):
    """bars: 완료 일봉만. 반환: 신호등 리스트 + 요약."""
    closes = [b[4] for b in bars]
    price = closes[-1] if closes else None
    lights = [check_trend(closes), check_heat(closes), check_flow(trend_rows, price),
              check_volume(bars), check_position(bars)]
    counts = {k: sum(l["color"] == k for l in lights) for k in (GREEN, YELLOW, RED, GREY)}
    return {"lights": lights, "counts": counts, "risk": risk_box(bars)}


# ── 진단 (수집 + 순수 함수 조합) ──────────────────────
def info_map(integration):
    return {i["code"]: i["value"] for i in integration.get("totalInfos", [])}


def diagnose(code):
    if not CODE_RE.match(code):
        raise ValueError("종목코드는 6자리(숫자/영문 대문자)입니다.")
    errors = []

    def safe(label, fn, default):
        try:
            return fn()
        except Exception as e:  # 한 패널 실패가 전체 페이지를 깨지 않게
            errors.append(f"{label} 조회 실패: {e}")
            return default

    base = f"https://m.stock.naver.com/api/stock/{code}"
    basic = safe("현재가", lambda: _get(f"{base}/basic"), {})
    integ = safe("종목정보", lambda: _get(f"{base}/integration"), {})
    trend_rows = safe("수급", lambda: _get(f"{base}/trend?pageSize=20"), [])
    bars = safe("일봉", lambda: fetch_daily(code), [])
    if not bars and not basic:
        raise ValueError("종목을 찾을 수 없거나 데이터를 가져오지 못했습니다.")

    # 장중이면 마지막 일봉은 형성 중 → 신호 계산에서 제외
    live = basic.get("marketStatus") == "OPEN"
    closed = bars[:-1] if live and bars and bars[-1][0] == date.today() else bars
    last_day = f"{closed[-1][0]:%Y%m%d}" if closed else ""
    result = diagnose_from(closed, [r for r in trend_rows if r.get("bizdate", "") <= last_day])

    info = info_map(integ)
    cons = integ.get("consensusInfo") or {}
    # 우선주(코드 끝자리 0 아님)는 네이버가 보통주 컨센서스를 그대로 붙여 줘서 괴리율이 왜곡된다 → 제외
    if cons.get("itemCode") != code or not code.endswith("0"):
        cons = {}
    target = num(cons.get("priceTargetMean"))
    last_close = closed[-1][4] if closed else None
    ref = {
        "PER": info.get("per"), "추정PER": info.get("cnsPer"), "PBR": info.get("pbr"),
        "배당수익률": info.get("dividendYieldRatio"), "시가총액": info.get("marketValue"),
        "컨센서스 목표가": f"{target:,.0f}원 (현재가 대비 {(target / last_close - 1) * 100:+.0f}%)"
        if target and last_close else None,
        "투자의견 평균(5점)": cons.get("recommMean"),
    }
    return {
        "code": code,
        "name": basic.get("stockName") or integ.get("stockName") or code,
        "market": (basic.get("stockExchangeType") or {}).get("name", ""),
        "is_etf": integ.get("stockEndType") == "etf",
        "price_now": basic.get("closePrice"),
        "change_pct": f"{(closed[-1][4] / closed[-2][4] - 1) * 100:.2f}" if len(closed) >= 2 else None,
        "last_close": closed[-1][4] if closed else None,
        "market_status": basic.get("marketStatus"),
        "traded_at": basic.get("localTradedAt"),
        "basis": f"신호는 KRX 정규장 완료 일봉 기준 (마지막 {closed[-1][0]:%Y-%m-%d}, {len(closed)}개)"
                 + (" · 장중이라 오늘 봉 제외" if live else "") if closed else "일봉 없음",
        "chart": [{"d": f"{b[0]:%Y-%m-%d}", "c": b[4]} for b in closed[-180:]],
        **result,
        "reference": {k: v for k, v in ref.items() if v and v != "N/A"},
        "researches": [{"date": r.get("wdt"), "broker": r.get("bnm"), "title": r.get("tit")}
                       for r in (integ.get("researches") or [])[:5]],
        "errors": errors,
    }


# ── 웹 ───────────────────────────────────────────────
def create_app():
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse

    app = FastAPI(title="주식 나침반", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/", response_class=HTMLResponse)
    def index():
        return PAGE.read_text(encoding="utf-8")

    @app.get("/api/search")
    def api_search(q: str):
        q = q.strip()
        if not q or len(q) > 30:
            return []
        if CODE_RE.match(q.upper()):
            return [{"code": q.upper(), "name": q.upper(), "market": ""}]
        return search(q)

    from pydantic import BaseModel, Field
    from scripts import kr_risk

    class SizeReq(BaseModel):
        code: str
        account: float = Field(gt=0, lt=1e13)
        risk_pct: float = Field(gt=0, le=10)
        max_pos_pct: float = Field(20, gt=0, le=100)
        cash: float | None = Field(None, ge=0, lt=1e13)
        entry: float | None = Field(None, gt=0)
        stop: float | None = Field(None, gt=0)
        atr_mult: float = Field(2.0, gt=0, le=10)

    @app.post("/api/size")  # 금액은 POST 본문으로만 (URL·로그에 남지 않게)
    def api_size(req: SizeReq):
        try:
            return kr_risk.size_for(req.code.upper(), req.account, req.risk_pct, req.max_pos_pct,
                                    req.cash, req.entry, req.stop, req.atr_mult)
        except ValueError as e:
            raise HTTPException(400, str(e))
        except httpx.HTTPError as e:
            raise HTTPException(502, f"데이터 조회 실패: {e}")

    @app.get("/api/portfolio")
    def api_portfolio_get():
        return kr_risk.load_portfolio()

    @app.put("/api/portfolio")
    def api_portfolio_put(body: dict):
        try:
            return kr_risk.save_portfolio(body)
        except (ValueError, TypeError) as e:
            raise HTTPException(400, str(e))

    @app.get("/api/portfolio/analysis")
    def api_portfolio_analysis():
        return kr_risk.analyze_saved()

    @app.get("/api/diagnose/{code}")
    def api_diagnose(code: str):
        try:
            return diagnose(code)
        except ValueError as e:
            raise HTTPException(400, str(e))
        except httpx.HTTPError as e:
            raise HTTPException(502, f"데이터 조회 실패: {e}")

    return app


def main():
    p = argparse.ArgumentParser(description="주식 나침반 로컬 웹앱")
    p.add_argument("--host", default="127.0.0.1",
                   help="휴대폰에서 보려면 0.0.0.0 — 주의: 같은 네트워크의 누구나 인증 없이 보유 내역을 보고 수정할 수 있음")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--open", action="store_true", help="실행 후 브라우저로 열기")
    a = p.parse_args()
    import uvicorn
    if a.open:
        import threading
        import webbrowser
        threading.Timer(1.5, webbrowser.open, (f"http://127.0.0.1:{a.port}",)).start()
    uvicorn.run(create_app(), host=a.host, port=a.port)


if __name__ == "__main__":
    main()
