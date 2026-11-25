"""폐기한 '국내 주식 추천 3선'(8766 사이트) 규칙의 일봉 근사 검증 — 폐기 근거 보존용.

규칙: 대형주 16개 중 전일 거래대금(종가×거래량) 상위 3개 → 다음 날 전일 고가 돌파 시 진입,
손절 = 전일 저가, 목표 = 진입 + 2R.
근사: 진입가 = max(다음날 시가, 전일 고가) (실제 규칙은 5분봉 종가 돌파라 진입가가 더 높음 → 이 근사는 낙관적).
같은 봉에서 손절·목표 동시 도달은 설정에 따라 처리. 비용은 왕복 % 차감.

실행: .venv/bin/python -m scripts.kr_breakout_review   (data/_kr_cache 사용, 없으면 수집)
결과: docs/kr_breakout_review_2026-11-18.txt
"""
from collections import Counter
from datetime import date
import statistics as st

from scripts import kr_compass_backtest as bt

UNIVERSE = {
    "005930": "삼성전자", "000660": "SK하이닉스", "005380": "현대차", "000270": "기아",
    "035420": "NAVER", "035720": "카카오", "051910": "LG화학", "006400": "삼성SDI",
    "005490": "POSCO홀딩스", "012330": "현대모비스", "105560": "KB금융", "055550": "신한지주",
    "247540": "에코프로비엠", "086520": "에코프로", "196170": "알테오젠", "058470": "리노공업",
}


def ranked(px, by, d, need_ahead=1):
    vals = []
    for c, bars in px.items():
        i = by[c].get(d)
        if i is not None and i >= 21 and i + need_ahead < len(bars):
            vals.append((bars[i][4] * bars[i][5], c, i))
    return sorted(vals, reverse=True)[:3] if len(vals) >= 3 else []


def run(px, by, cal, hold, target_first, cost):
    res, both = {"IS": [], "OOS": []}, 0
    for d in cal[60:-1]:
        seg = "IS" if d <= bt.IS_END else "OOS"
        for _, c, i in ranked(px, by, d, hold):
            ref, nxt = px[c][i], px[c][i + 1]
            if nxt[2] <= ref[2] or ref[2] <= ref[3]:
                continue
            entry, stop = max(nxt[1], ref[2]), ref[3]
            risk = entry - stop
            target, out = entry + 2 * risk, None
            for j in range(i + 1, i + 1 + hold):
                b = px[c][j]
                s, t = b[3] <= stop, b[2] >= target
                if s and t:
                    both += 1
                    out = target if target_first else stop
                elif s:
                    out = stop
                elif t:
                    out = target
                if out is not None:
                    break
            if out is None:
                out = px[c][i + hold][4]
            res[seg].append((out / entry - 1) * 100 - cost)
    return res, both


def main():
    bt.CACHE.mkdir(parents=True, exist_ok=True)
    px = {c: bt.prices(c) for c in UNIVERSE}
    by = {c: {b[0]: i for i, b in enumerate(bars)} for c, bars in px.items()}
    cal = [b[0] for b in bt.prices("069500")]
    for c, bars in px.items():
        print(f"  {c} {UNIVERSE[c]}: {bars[0][0]} ~ {bars[-1][0]}, {len(bars)}행")

    cnt, days = Counter(), 0
    for d in cal[-250:]:
        top = ranked(px, by, d, 0)
        days += bool(top)
        cnt.update(UNIVERSE[c] for _, c, _ in top)
    print(f"\n최근 {days}거래일 상위 3 등장 횟수: {dict(cnt.most_common())}")

    print(f"\n거래당 평균 수익률 (IS ~{bt.IS_END} / OOS 이후), 괄호 = t값, 거래 수")
    for hold in (1, 10):
        for tf in (False, True):
            for cost in (0, 0.25):
                res, both = run(px, by, cal, hold, tf, cost)
                cells = " · ".join(f"{k} {st.mean(v):+.3f}% (t{st.mean(v) / (st.stdev(v) / len(v) ** .5):+.1f}, n{len(v)})"
                                   for k, v in res.items())
                print(f"  최대 보유 {hold:>2}일 · 동시도달 {'목표' if tf else '손절'} 우선 · 비용 {cost}% → {cells} · 동시도달 {both}건")
    print("\n주의: 유니버스가 현재 대형 승자 16개라 생존편향으로 결과가 낙관적임.")


if __name__ == "__main__":
    main()
