"""주식 나침반 신호등 검증 (IS/OOS).

앱(scripts/kr_compass.py)의 check_* 함수를 그대로 호출해, 신호 후 h거래일 수익률이
같은 날 유니버스 평균보다 나았는지(초과수익)를 신호등 색깔별로 측정한다.

- 진입: 신호일 t 종가 확인 → t+1 시가 진입, t+h 종가 청산 (h = 5, 10, 20)
- 평가: 날짜별 단면 평균(초과수익) → 날짜에 걸쳐 평균, t값은 날짜 수 기준 (종목-일 아님)
- 표본: h거래일 간격의 겹치지 않는 날짜
- 분할: IS 2012~2019 / OOS 2020~ (고정, 결과 보기 전에 정함)
- 한계: 유니버스 = 현재 시총 상위 → 생존편향(상폐·몰락 종목 누락). 빨강 쪽 결과를 특히 낙관적으로 만든다.

실행: .venv/bin/python -m scripts.kr_compass_backtest [--kospi 150 --kosdaq 100] [--flow N]
"""
import argparse
from collections import defaultdict
from datetime import date, timedelta
import json
import math
from pathlib import Path
import statistics as st
import time

import httpx

from scripts import kr_compass as k

CACHE = Path(__file__).resolve().parents[1] / "data/_kr_cache"
START = "20120101"
WINDOW = 280            # 앱이 보는 일봉 길이와 동일하게
STEP = 5                # 표본 간격(거래일). h는 STEP의 배수
HORIZONS = (5, 10, 20)
IS_END = date(2019, 12, 31)
COST_RT = 0.4           # 왕복 비용 가정(%) — 거래세+수수료+슬리피지
LIGHTS = ("trend", "heat", "volume", "position")
client = httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=20)


# ── 수집 (캐시, 재개 가능) ───────────────────────────
def cached(name, fetch):
    path = CACHE / name
    if path.exists():
        return json.loads(path.read_text())
    data = fetch()
    path.write_text(json.dumps(data, ensure_ascii=False))
    time.sleep(0.15)
    return data


def universe(n_kospi, n_kosdaq):
    out = []
    for mkt, n in (("KOSPI", n_kospi), ("KOSDAQ", n_kosdaq)):
        page, got = 1, []
        while len(got) < n:
            url = f"https://m.stock.naver.com/api/stocks/marketValue/{mkt}?page={page}&pageSize=100"
            stocks = cached(f"uni_{mkt}_{page}.json", lambda: client.get(url).json()["stocks"])
            if not stocks:
                break
            got += [(s["itemCode"], s["stockName"]) for s in stocks if s.get("stockEndType") == "stock"]
            page += 1
        out += got[:n]
    return out


def prices(code):
    def fetch():
        url = (f"https://api.finance.naver.com/siseJson.naver?symbol={code}&requestType=1"
               f"&startTime={START}&endTime={date.today():%Y%m%d}&timeframe=day")
        return json.loads(client.get(url).text.replace("'", '"'))
    return k.parse_daily(cached(f"px_{code}.json", fetch))


def flows(code, since):
    """수급 이력. bizdate 페이지네이션(60행/호출)으로 since까지 거슬러 올라감."""
    def fetch():
        rows, cursor = [], None
        while True:
            url = f"https://m.stock.naver.com/api/stock/{code}/trend?pageSize=60" + (f"&bizdate={cursor}" if cursor else "")
            batch = client.get(url).json()
            time.sleep(0.1)
            new = [r for r in batch if not rows or r["bizdate"] < rows[-1]["bizdate"]]
            if not new:
                break
            rows += new
            if new[-1]["bizdate"] <= since:
                break
            cursor = new[-1]["bizdate"]
        return rows
    return cached(f"flow_{code}_{since}.json", fetch)


# ── 거래량 액면분할 보정 부재 처리 ───────────────────
def split_suspects(bars):
    """수정주가인데 거래량은 미수정 → 분할일 전후 거래량 수준이 계단식으로 바뀐다.
    전후 20일 중앙값 비율이 8배 이상이면 분할 의심일로 보고 인덱스 반환."""
    v = [b[5] for b in bars]
    out = []
    for i in range(20, len(v) - 20):
        before, after = st.median(v[i - 20:i]), st.median(v[i:i + 20])
        if before > 0 and (after / before >= 8 or before / after >= 8):
            out.append(i)
    return out


# ── 평가 ────────────────────────────────────────────
def evaluate(bars, calendar, flow_rows=None):
    """[(date, {light: color}, {h: fwd_return_pct})]
    calendar: {date: 거래일 번호} — 모든 종목이 같은 날짜에 표본을 찍도록 달력 기준으로 샘플링."""
    splits = split_suspects(bars)
    flow_by_date = {r["bizdate"]: r for r in (flow_rows or [])}
    flow_dates = sorted(flow_by_date, reverse=True)
    out = []
    for t in range(WINDOW - 1, len(bars) - 1):
        n = calendar.get(bars[t][0])
        if n is None or n % STEP:
            continue
        window = bars[t - WINDOW + 1:t + 1]
        closes = [b[4] for b in window]
        colors = {
            "trend": k.check_trend(closes)["color"],
            "heat": k.check_heat(closes)["color"],
            "position": k.check_position(window)["color"],
        }
        if not any(t - 21 <= s <= t + 1 for s in splits):
            colors["volume"] = k.check_volume(window)["color"]
        if flow_rows:
            d = f"{bars[t][0]:%Y%m%d}"
            recent = [flow_by_date[x] for x in flow_dates if x <= d][:5]
            if len(recent) == 5 and recent[0]["bizdate"] == d:
                colors["flow"] = k.check_flow(recent, bars[t][4])["color"]
        fwd = {}
        for h in HORIZONS:
            if t + h < len(bars) and n % h == 0:
                fwd[h] = (bars[t + h][4] / bars[t + 1][1] - 1) * 100
        out.append((bars[t][0], colors, fwd))
    return out


def tstat(xs):
    if len(xs) < 3:
        return float("nan")
    sd = st.stdev(xs)
    return st.mean(xs) / (sd / math.sqrt(len(xs))) if sd else float("nan")


def report(samples, lights, title):
    """samples: [(date, colors, fwd)] 전 종목 합본."""
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")
    verdict = {}
    for h in HORIZONS:
        by_date = defaultdict(list)
        for d, colors, fwd in samples:
            if h in fwd:
                by_date[d].append((colors, fwd[h]))
        print(f"\n[h={h}일] 표본 날짜 {len(by_date)}개 · 값=날짜별 (색 평균 − 전체 평균) 의 평균 %, (t값)")
        print(f"{'신호등':<10}{'구간':<6}{'초록':>14}{'주황':>14}{'빨강':>14}{'초록−빨강':>16}{'날짜':>6}")
        for light in lights:
            for label, lo, hi in (("IS", date(2000, 1, 1), IS_END), ("OOS", IS_END + timedelta(1), date(2100, 1, 1))):
                ex = {c: [] for c in (k.GREEN, k.YELLOW, k.RED)}
                spread = []
                for d, rows in by_date.items():
                    if not lo <= d <= hi:
                        continue
                    rows = [(c[light], r) for c, r in rows if light in c and c[light] != k.GREY]
                    if len(rows) < 10:
                        continue
                    base = st.mean(r for _, r in rows)
                    means = {}
                    for col in ex:
                        vals = [r for c, r in rows if c == col]
                        if len(vals) >= 3:
                            means[col] = st.mean(vals) - base
                            ex[col].append(means[col])
                    if k.GREEN in means and k.RED in means:
                        spread.append(means[k.GREEN] - means[k.RED])
                cell = lambda xs: f"{st.mean(xs):+.2f}({tstat(xs):+.1f})" if len(xs) >= 3 else "-"
                print(f"{light:<10}{label:<6}{cell(ex[k.GREEN]):>14}{cell(ex[k.YELLOW]):>14}{cell(ex[k.RED]):>14}"
                      f"{cell(spread):>16}{len(spread):>6}")
                verdict.setdefault(light, {})[(h, label)] = (st.mean(spread) if len(spread) >= 3 else None, tstat(spread))
    return verdict


def per_year(samples, light, h=10):
    by_date = defaultdict(list)
    for d, colors, fwd in samples:
        if h in fwd and light in colors and colors[light] != k.GREY:
            by_date[d].append((colors[light], fwd[h]))
    yearly = defaultdict(list)
    for d, rows in by_date.items():
        g = [r for c, r in rows if c == k.GREEN]
        r_ = [r for c, r in rows if c == k.RED]
        if len(g) >= 3 and len(r_) >= 3:
            yearly[d.year].append(st.mean(g) - st.mean(r_))
    return " ".join(f"{y}:{st.mean(v):+.1f}" for y, v in sorted(yearly.items()))


def tally(samples, h=10):
    """초록 개수(가격 신호등 4개 중)별 초과수익 — 앱의 '충족 N' 요약이 의미 있는지."""
    by_date = defaultdict(list)
    for d, colors, fwd in samples:
        if h in fwd and all(l in colors for l in LIGHTS):
            by_date[d].append((sum(colors[l] == k.GREEN for l in LIGHTS), fwd[h]))
    print(f"\n[초록 개수별, h={h}] 값=날짜별 초과수익 평균 %, (t값), 날짜 수")
    for label, lo, hi in (("IS", date(2000, 1, 1), IS_END), ("OOS", IS_END + timedelta(1), date(2100, 1, 1))):
        cols = []
        for n in range(5):
            xs = []
            for d, rows in by_date.items():
                if lo <= d <= hi and len(rows) >= 10:
                    base = st.mean(r for _, r in rows)
                    vals = [r for c, r in rows if c == n]
                    if len(vals) >= 3:
                        xs.append(st.mean(vals) - base)
            cols.append(f"{n}개 {st.mean(xs):+.2f}({tstat(xs):+.1f},{len(xs)})" if len(xs) >= 3 else f"{n}개 -")
        print(f"  {label:<4}" + "  ".join(cols))


def heat_split(uni, calendar, h=10):
    """[사후 분석 — 결과를 본 뒤 추가] 과열 신호등 빨강을 과매도(RSI<30)와 과열(RSI>70·이격>12%)로 분해."""
    by_date = defaultdict(list)
    for code, _ in uni:
        bars = prices(code)
        if len(bars) < WINDOW + 30:
            continue
        for t in range(WINDOW - 1, len(bars) - h):
            n = calendar.get(bars[t][0])
            if n is None or n % h:
                continue
            c = [b[4] for b in bars[t - WINDOW + 1:t + 1]]
            r, gap = k.rsi(c), (c[-1] / k.sma(c, 20) - 1) * 100
            cat = ("과매도" if r < 30 else "과열" if (r > 70 or gap > 12)
                   else "초록" if (40 <= r <= 65 and gap <= 8) else "주황")
            by_date[bars[t][0]].append((cat, (bars[t + h][4] / bars[t + 1][1] - 1) * 100))
    print(f"\n[사후 분석: 과열 신호등 분해, h={h}] 값=날짜별 초과수익 평균 %, (t값, 날짜 수)")
    for label, lo, hi in (("IS", date(2000, 1, 1), IS_END), ("OOS", IS_END + timedelta(1), date(2100, 1, 1))):
        out = defaultdict(list)
        for d, rows in by_date.items():
            if lo <= d <= hi and len(rows) >= 10:
                base = st.mean(r for _, r in rows)
                for cat in {c for c, _ in rows}:
                    v = [r for c, r in rows if c == cat]
                    if len(v) >= 3:
                        out[cat].append(st.mean(v) - base)
        print(f"  {label:<4}" + "  ".join(f"{c} {st.mean(x):+.2f}({tstat(x):+.1f},{len(x)})" for c, x in sorted(out.items())))


def conclude(verdict):
    print(f"\n[판정] 기준: 초록−빨강 격차가 모든 h·IS·OOS에서 같은 부호 & OOS h=10 |t|≥2 & 격차 > 비용 {COST_RT}%")
    for light, cells in verdict.items():
        vals = [v for v, _ in cells.values() if v is not None]
        same = len(vals) == len(cells) and (all(v > 0 for v in vals) or all(v < 0 for v in vals))
        oos_v, oos_t = cells.get((10, "OOS"), (None, float("nan")))
        ok = same and oos_v is not None and abs(oos_t) >= 2 and abs(oos_v) > COST_RT
        sign = "정방향" if vals and vals[0] > 0 else "역방향"
        print(f"  {light:<10} {'지지됨 (' + sign + ')' if ok else '근거 부족/효과 없음'}"
              f"  · OOS h=10 격차 {oos_v if oos_v is None else round(oos_v, 2)}%, t={oos_t:.1f}, 부호일관={same}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kospi", type=int, default=150)
    ap.add_argument("--kosdaq", type=int, default=100)
    ap.add_argument("--flow", type=int, default=0, help="수급 검증용 종목 수(시총 상위). 0이면 생략")
    ap.add_argument("--flow-since", default="20160101")
    a = ap.parse_args()
    CACHE.mkdir(parents=True, exist_ok=True)

    uni = universe(a.kospi, a.kosdaq)
    calendar = {b[0]: i for i, b in enumerate(prices("069500"))}  # KODEX 200 거래일을 달력으로
    print(f"유니버스 {len(uni)}종목 (현재 시총 상위 — 생존편향 있음)")
    all_samples, flow_samples, short = [], [], 0
    for i, (code, name) in enumerate(uni):
        try:
            bars = prices(code)
        except Exception as e:
            print(f"  {code} {name} 수집 실패: {e}")
            continue
        if len(bars) < WINDOW + 30:
            short += 1
            continue
        if i < 5 or i % 50 == 0:
            print(f"  {code} {name}: {bars[0][0]} ~ {bars[-1][0]}, {len(bars)}행, 분할의심 {len(split_suspects(bars))}일")
        all_samples += evaluate(bars, calendar)
        if i < a.flow:
            fl = flows(code, a.flow_since)
            if fl:
                flow_samples += [s for s in evaluate(bars, calendar, fl) if "flow" in s[1]]
    print(f"이력 부족 제외 {short}종목 · 표본(종목-날짜) {len(all_samples):,}")

    verdict = report(all_samples, LIGHTS, "가격 신호등 4종 · 신호 색깔별 h일 초과수익")
    print("\n[연도별 초록−빨강 격차 %, h=10]")
    for light in LIGHTS:
        print(f"  {light:<10} {per_year(all_samples, light)}")
    tally(all_samples)
    if flow_samples:
        d0 = min(s[0] for s in flow_samples)
        print(f"\n수급 표본 {len(flow_samples):,} · {d0} ~ {max(s[0] for s in flow_samples)}")
        verdict.update(report(flow_samples, ("flow",), f"수급 신호등 · 상위 {a.flow}종목"))
        print(f"  flow       {per_year(flow_samples, 'flow')}")
    conclude(verdict)
    heat_split(uni, calendar)


if __name__ == "__main__":
    main()
