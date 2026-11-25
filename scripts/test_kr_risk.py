"""리스크 계산 단위테스트 (네트워크 없음).  실행: .venv/bin/python -m scripts.test_kr_risk"""
from datetime import date, timedelta
import math
import random

from scripts import kr_risk as r


def bars_from(closes, start=date(2025, 1, 1)):
    return [(start + timedelta(days=i), c, c, c, c, 1000.0) for i, c in enumerate(closes)]


def test_sizing():
    # 손계산: 1억, 1% → 위험예산 100만. 진입 50,000 손절 45,000 → 주당 5,000 → 200주(1,000만, 10%)
    s = r.size_position(100_000_000, 1.0, 50_000, 45_000, max_pos_pct=20)
    assert s["shares"] == 200 and s["binding"] == "위험 한도", s
    assert s["loss_at_stop"] == 1_000_000 and math.isclose(s["position_pct"], 10.0)
    assert math.isclose(s["loss_limit_down"], 3_000_000)
    # 최대 비중이 묶는 경우: 손절폭 1,000 → 위험한도 1000주(5천만) > 비중 20%(400주)
    s = r.size_position(100_000_000, 1.0, 50_000, 49_000, max_pos_pct=20)
    assert s["shares"] == 400 and s["binding"] == "최대 비중"
    # 현금이 묶는 경우
    s = r.size_position(100_000_000, 1.0, 50_000, 45_000, cash=3_000_000)
    assert s["shares"] == 60 and s["binding"] == "가용 현금"
    # 0주 / 잘못된 손절
    assert r.size_position(1_000_000, 0.1, 500_000, 400_000)["state"] == "zero"
    assert r.size_position(1_000_000, 1, 50_000, 50_000)["state"] == "error"
    assert r.size_position(1_000_000, 1, 50_000, None)["state"] == "error"


def test_portfolio():
    random.seed(1)
    walk = [100.0]
    for _ in range(200):
        walk.append(walk[-1] * (1 + random.gauss(0, 0.02)))
    mirror = [100.0]
    for a, b in zip(walk, walk[1:]):
        mirror.append(mirror[-1] * (1 - (b / a - 1)))
    A = {"code": "000001", "name": "A", "shares": 100, "bars": bars_from(walk)}
    same = {"code": "000002", "name": "B", "shares": 100, "bars": bars_from(walk)}
    mir = {"code": "000003", "name": "C", "shares": round(100 * walk[-1] / mirror[-1]), "bars": bars_from(mirror)}

    one = r.portfolio_stats([A])["stats"]["vol_daily"]
    two = r.portfolio_stats([A, same])["stats"]
    assert math.isclose(two["vol_daily"], one, rel_tol=1e-9)  # 동일 자산 → 가중평균 변동성
    assert math.isclose(two["corr"]["000001"]["000002"], 1.0)
    hedged = r.portfolio_stats([A, mir])["stats"]
    assert hedged["vol_daily"] < one * 0.05, hedged["vol_daily"]  # 거울 자산 → 거의 0
    assert hedged["corr"]["000001"]["000003"] < -0.99

    # 현금이 분모에 포함 → 비중·변동성 절반
    half = r.portfolio_stats([A], cash=A["shares"] * walk[-1])
    assert math.isclose(half["holdings"][0]["weight"], 50.0)
    assert math.isclose(half["stats"]["vol_daily"], one / 2, rel_tol=1e-9)

    # 손절 없는 종목은 위험 합산에서 빠지고 '미정'
    p = r.portfolio_stats([{**A, "stop": walk[-1] * 0.9}, same])
    assert math.isclose(p["open_risk"], 100 * walk[-1] * 0.1) and p["risk_undefined"] == ["B"]

    # 날짜 교집합 정렬: 신규 상장(30일) → 통계 생략
    new = {"code": "000004", "name": "N", "shares": 10, "bars": bars_from(walk[-30:], date(2025, 6, 20))}
    p = r.portfolio_stats([A, new])
    assert p["stats"] is None and "60일 미만" in p["stats_note"]

    # 손절선 이탈 경보
    p = r.portfolio_stats([{**A, "stop": walk[-1] + 1}])
    assert any("손절선 이탈" in x for x in p["holdings"][0]["alerts"])


def test_validate():
    ok = r.validate_portfolio({"cash": "1000", "holdings": [{"code": "005930", "shares": "3", "stop": ""}]})
    assert ok["holdings"][0] == {"code": "005930", "name": "005930", "shares": 3}
    for bad in ({"holdings": [{"code": "x", "shares": 1}]}, {"holdings": [{"code": "005930", "shares": 0}]},
                {"cash": -1}, {"holdings": [{"code": "005930", "shares": 1}] * 51}):
        try:
            r.validate_portfolio(bad)
        except ValueError:
            continue
        raise AssertionError(bad)


if __name__ == "__main__":
    test_sizing()
    test_portfolio()
    test_validate()
    print("OK")
