"""미국 주식 장중 돌파 후보와 조건부 매도 시점을 확인한다."""
import argparse
from datetime import timedelta
from decimal import Decimal
import importlib.util
from pathlib import Path
import re
import time

TOOLS = Path(__file__).resolve().parents[1] / "external/day-trader-skill"
DEFAULT_SYMBOLS = "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD"


def load_engine(folder, name):
    path = TOOLS / folder / (name + ".py")
    if not path.is_file():
        raise SystemExit(f"분석 엔진 설치가 필요합니다. README의 미국 주식 도우미 설치 절차를 확인하세요. ({path})")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def evaluate_symbol(entry, exit_plan, chart, now):
    result = entry.evaluate(chart, now)
    bars = result["_bars"]
    phase = result["phase"]
    if phase == "closed":
        return "휴장", "다음 정규장에 확인"
    if phase == "pre-open":
        return "개장 전", "09:30부터 첫 15분의 가격 범위 관찰"
    if not bars:
        return "자료 부족", "당일 완료된 5분봉 없음"
    latest = bars[-1]["end"]
    stamp = latest.strftime("%H:%M")
    # A missing bar can hide either the first breakout or the first exit.
    if any(bar["slot"] != index for index, bar in enumerate(bars)):
        return "자료 부족", f"{stamp}까지 5분봉 누락"
    stale = entry.to_eastern(now) - latest >= timedelta(minutes=6)
    if phase == "forming":
        return "관찰 중", f"{stamp} 기준 개장 범위 형성 중"

    trigger = result["trigger"]
    if not trigger:
        status = "진입 마감" if result["status"] == "expired" else "돌파 대기"
        session_end = latest.replace(hour=int(result["no_entries_after"][:2]), minute=int(result["no_entries_after"][3:]))
        if stale and latest < session_end:
            status = "후속 시세 대기"
        return status, f"{stamp} 종가 ${bars[-1]['c']:.2f} · 돌파 기준 ${result['level']:.2f}"

    opening = result["opening_range"]
    plan = exit_plan.compute(trigger["price"], "or_mid", None, "r", Decimal("2"),
                             or_high=opening["high"], or_low=opening["low"])
    deadline, _ = exit_plan.resolve_flat_by(latest.date(), None, {}, None)
    status = "돌파 후보"
    detail = (f"{trigger['close_time']} 종가 ${plan['entry']:.2f} > 개장 고가 ${opening['high']:.2f}"
              f" · 손절 ${plan['stop']:.2f} · 목표 ${plan['target']:.2f} · 시간 청산 {deadline}")
    for bar in bars[trigger["bar_index"] + 1:]:
        stop = bar["l"] <= float(plan["stop"])
        target = bar["h"] >= float(plan["target"])
        ended = bar["end"].strftime("%H:%M")
        if stop and target:
            status = "손절·목표 모두 도달 (순서 불명)"
        elif stop:
            status = "손절선 도달"
        elif target:
            status = "목표가 도달"
        elif ended >= deadline:
            status = "시간 청산"
        else:
            continue
        return status, f"{detail} · {ended} 종료 봉에서 확인"
    if trigger["bar_index"] != bars[-1]["slot"]:
        status = "기존 신호 추적"
    if stale:
        status = "후속 시세 대기"
    return status, f"{detail} · 최종 봉 {stamp}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS, help="쉼표로 구분한 미국 주식 티커")
    parser.add_argument("--demo", action="store_true", help="저장된 TSLA 거래일 재생")
    parser.add_argument("--watch", action="store_true", help="60초마다 갱신")
    args = parser.parse_args()
    if args.demo and args.watch:
        parser.error("데모는 고정 시점 재생입니다.")
    symbols = list(dict.fromkeys(t.strip().upper() for t in args.symbols.split(",")))
    if not all(re.fullmatch(r"[A-Z]{1,5}(?:[.-][A-Z])?", s) for s in symbols):
        parser.error("--symbols에 미국 주식 티커를 입력하세요.")
    entry = load_engine("trader-entry", "entry")
    exit_plan = load_engine("trader-exit", "exit")
    offline = None
    if args.demo:
        symbols = ["TSLA"]
        offline = str(TOOLS / "tests/fixtures/trader-entry/TSLA-2026-10-19-5m.json")
    while True:
        now = entry.parse_now("2026-10-19 10:15") if args.demo else entry.utc_now()
        mode = "과거 재생" if args.demo else "Yahoo Finance 지연 가능"
        print(f"\n{entry.to_eastern(now):%Y-%m-%d %H:%M} 뉴욕 | {mode}", flush=True)
        successes = 0
        for symbol in symbols:
            try:
                chart = entry.get_chart(symbol, "5m", offline=offline)
                status, detail = evaluate_symbol(entry, exit_plan, chart, now)
                successes += 1
            except (entry.EntryError, exit_plan.PlanError, OSError, ValueError) as error:
                status, detail = "분석 불가", str(error)
            print(f"{symbol:<6} [{status}] {detail}", flush=True)
        if not args.watch:
            return 0 if successes else 1
        time.sleep(60)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
