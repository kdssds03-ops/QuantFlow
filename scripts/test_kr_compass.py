"""주식 나침반 순수 함수 단위테스트 (네트워크 없음).  실행: .venv/bin/python -m scripts.test_kr_compass"""
from datetime import date, timedelta

from scripts import kr_compass as k


def bars_from(closes, vol=1000.0):
    d0 = date(2025, 1, 1)
    return [(d0 + timedelta(days=i), c, c * 1.01, c * 0.99, c, vol) for i, c in enumerate(closes)]


def flow(f, o):
    return {"foreignerPureBuyQuant": f"{f:+,}", "organPureBuyQuant": f"{o:+,}"}


def test():
    assert k.num("+1,104,534") == 1104534 and k.num("8.81배") == 8.81
    assert k.num("5조 3,553억") is None and k.num("N/A") is None

    up = [100 + i * 0.5 for i in range(300)]
    down = [250 - i * 0.5 for i in range(300)]
    assert k.check_trend(up)["color"] == k.GREEN
    assert k.check_trend(down)["color"] == k.RED
    assert k.check_trend(up[:30])["color"] == k.GREY  # 자료 부족은 회색

    assert k.rsi([100.0] * 10) is None
    assert k.rsi(up) == 100.0  # 손실 없음
    assert k.check_heat(up)["color"] == k.RED  # 계속 상승 → 과열

    assert k.check_flow([flow(100, 50)] * 5, 1000)["color"] == k.GREEN
    assert k.check_flow([flow(-100, -50)] * 5, 1000)["color"] == k.RED
    assert k.check_flow([flow(1, 1)] * 3, 1000)["color"] == k.GREY

    b = bars_from(up)
    b[-1] = (*b[-1][:5], 5000.0)  # 상승일 거래량 5배
    assert k.check_volume(b)["color"] == k.GREEN
    assert k.check_position(bars_from(up))["color"] in (k.YELLOW, k.GREEN)
    assert k.check_position(bars_from(down))["color"] == k.RED
    assert k.check_position([])["color"] == k.GREY

    empty = k.diagnose_from([], [])
    assert empty["counts"][k.GREY] == 5 and empty["risk"] is None
    print("OK")


if __name__ == "__main__":
    test()
