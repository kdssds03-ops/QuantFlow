"""
scripts/test_order_safety.py — 라이브 주문 경로 안전장치 회귀 테스트 (거래소/DB 불필요)

2026-11-25 코드 감사에서 고친 결함이 되살아나지 않는지 확인한다.
  - 타임아웃 재시도 중복 주문 (clientOrderId 조회 후 재송신)
  - 청산 reduceOnly
  - cancel 실패 시 체결 재확인
  - 서킷브레이커 fail-closed
  - stepSize fallback 비캐시
  - TREND 예측기 예외 → HOLD

사용: .venv/bin/python -m pytest -q scripts/test_order_safety.py
"""
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import ccxt
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
# worker.tasks import 시 DB 워밍업이 실패해도 로그만 남고 import는 성공한다.
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6399/0")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")

import worker.tasks as t  # noqa: E402


@pytest.fixture(autouse=True)
def _no_sleep_no_telegram():
    with patch.object(t.time, "sleep"), patch.object(t, "notifier") as n:
        yield n


def _run(exchange, **kw):
    args = dict(
        exchange=exchange, symbol="BTC/USDT", side="BUY", amount=Decimal("0.01"),
        trigger_type="TEST", fallback_price=Decimal("100000"), confidence=1.0,
        usdt_balance=Decimal("1000"),
    )
    args.update(kw)
    return t._execute_order_pipeline(**args)


FILLED = {"id": "1", "status": "closed", "filled": 0.01, "average": 100010}


def test_timeout_but_order_exists_is_not_resent():
    ex = MagicMock()
    ex.create_order.side_effect = ccxt.RequestTimeout("timeout")
    ex.fetch_order.return_value = FILLED
    r = _run(ex)
    assert r["status"] == "FILLED"
    assert ex.create_order.call_count == 1  # 중복 주문 없음


def test_timeout_not_found_resends_with_same_client_id():
    ex = MagicMock()
    ex.create_order.side_effect = [ccxt.RequestTimeout("timeout"), FILLED]
    ex.fetch_order.side_effect = ccxt.OrderNotFound("none")
    r = _run(ex)
    assert r["status"] == "FILLED"
    ids = [c.kwargs["params"]["newClientOrderId"] for c in ex.create_order.call_args_list]
    assert len(ids) == 2 and ids[0] == ids[1]


def test_timeout_and_lookup_unreachable_fails_without_resend(_no_sleep_no_telegram):
    ex = MagicMock()
    ex.create_order.side_effect = ccxt.RequestTimeout("timeout")
    ex.fetch_order.side_effect = ccxt.NetworkError("down")
    r = _run(ex)
    assert r["status"] == "FAILED"
    assert ex.create_order.call_count == 1
    assert _no_sleep_no_telegram.send_message.called  # 무음 실패 금지


def test_exit_orders_are_reduce_only():
    ex = MagicMock()
    ex.create_order.return_value = FILLED
    _run(ex, reduce_only=True)
    assert ex.create_order.call_args.kwargs["params"]["reduceOnly"] is True
    _run(ex)
    assert "reduceOnly" not in ex.create_order.call_args.kwargs["params"]


def test_cancel_failure_rechecks_fill():
    ex = MagicMock()
    ex.create_order.return_value = {"id": "9", "status": "open", "filled": 0}
    ex.fetch_order.side_effect = [{"status": "open", "filled": 0}] * 5 + [FILLED]
    ex.cancel_order.side_effect = ccxt.OrderNotFound("Unknown order")
    r = _run(ex)
    assert r["status"] == "FILLED"
    assert r["filled_amount"] == Decimal("0.01")


def test_breaker_fails_closed_without_redis():
    with patch.object(t.settings, "max_daily_loss_pct", 0.05), patch.object(t, "_r_sync", None):
        assert t._check_daily_loss_breaker(Decimal("1000")) is True


def test_breaker_fails_closed_on_zero_equity():
    r = MagicMock()
    with patch.object(t.settings, "max_daily_loss_pct", 0.05), patch.object(t, "_r_sync", r):
        assert t._check_daily_loss_breaker(Decimal("0")) is True
    r.set.assert_not_called()  # 0을 기준 자본으로 기록하지 않음


def test_lot_size_fallback_not_cached():
    t._LOT_SIZE_CACHE.pop("SOL/USDT", None)
    ex = MagicMock()
    ex.market.side_effect = Exception("temporary")
    assert t._get_lot_size_step(ex, "SOL/USDT", fallback_step="0.001") == Decimal("0.001")
    assert "SOL/USDT" not in t._LOT_SIZE_CACHE
    ex.market.side_effect = None
    ex.market.return_value = {"precision": {"amount": 0.01}}
    assert t._get_lot_size_step(ex, "SOL/USDT") == Decimal("0.01")


def test_alert_throttled_sends_once():
    r = MagicMock()
    r.set.side_effect = [True, None]
    with patch.object(t, "_r_sync", r):
        t._alert_throttled("k", "m")
        t._alert_throttled("k", "m")
    assert t.notifier.send_message.call_count == 1


def test_trend_predictor_error_returns_hold():
    from worker.predictor import TrendFollowingPredictor

    def broken_session():
        raise RuntimeError("db down")

    p = TrendFollowingPredictor(session_factory=broken_session)
    assert p.predict_with_confidence({"symbol": "BTC/USDT"}) == ("HOLD", 0.0)
    assert TrendFollowingPredictor(session_factory=None).predict_with_confidence({}) == ("HOLD", 0.0)


def test_last_timeout_still_checks_existing_order():
    ex = MagicMock()
    ex.create_order.side_effect = ccxt.RequestTimeout("timeout")
    # 2·3번째 시도 전 조회: 없음 / 루프 종료 후 최종 조회: 체결 발견
    ex.fetch_order.side_effect = [ccxt.OrderNotFound("x"), ccxt.OrderNotFound("x"), FILLED]
    r = _run(ex)
    assert r["status"] == "FILLED"
    assert ex.create_order.call_count == 3
