"""
worker.ohlcv_stream — WebSocket 기반 인메모리 OHLCV 스트리밍 큐

Zero-I/O Latency 설계 원칙:
  - Binance WebSocket kline 스트림을 상시 구독하여 캔들 데이터를 수신
  - collections.deque(maxlen=500) 인메모리 큐에 캔들 보관
  - analyze_and_trade / fetch_market_data_task 가 큐를 직접 참조하여
    REST API 호출 없이 0ms 근접 데이터 접근
  - 연결 끊김 시 Exponential Backoff 자동 재연결 + REST Fallback 지원

사용 방법:
    # 백그라운드 스레드로 스트림 시작 (워커 임포트 시 1회)
    from worker.ohlcv_stream import ohlcv_stream_manager
    ohlcv_stream_manager.start()          # 논블로킹 백그라운드 스레드 시작

    # 태스크에서 인메모리 큐 직접 조회 (0ms)
    df = ohlcv_stream_manager.get_latest_df(symbol="BTC/USDT", min_candles=60)

아키텍처 노트:
    [Binance WS] → [asyncio event loop in Thread] → [deque(maxlen=500)]
                                                            ↓
    [analyze_and_trade Task] ← get_latest_df() ← [deque read, 0ms]
"""

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── WebSocket 엔드포인트 (Binance Futures USDM) ─────────────────────────
_WS_BASE_URL = "wss://fstream.binance.com/market/ws"
_WS_DEMO_URL = "wss://fstream.binancefuture.com/market/ws"

# ── 재연결 정책 (Exponential Backoff) ────────────────────────────────────
_RECONNECT_MIN_SEC   = 1     # 첫 재연결 대기 (초)
_RECONNECT_MAX_SEC   = 60    # 최대 대기 상한 (초)
_RECONNECT_BACKOFF   = 2.0   # 대기 시간 배수
_PING_INTERVAL_SEC   = 20    # WebSocket 핑 간격 (서버 타임아웃 방지)
_CANDLE_QUEUE_MAXLEN = 500   # 인메모리 보관 최대 캔들 수 (EMA50 warm-up 보장)


class OhlcvStreamManager:
    """
    Binance WebSocket kline 스트림 기반 인메모리 캔들 큐 관리자.

    설계 원칙:
    - WebSocket 연결은 전용 daemon 스레드에서 asyncio 루프를 구동
    - 캔들 데이터는 {symbol: deque} 딕셔너리에 보관
    - 외부 태스크는 get_latest_df()로 큐를 직접 참조 (락 없는 스냅샷)
    - REST Fallback: 큐가 비어있거나 min_candles 미달 시 ccxt로 자동 보완

    스레드 안전성:
    - deque는 CPython GIL에 의해 append/popleft가 원자적으로 처리됨
    - 외부 읽기(list(deque))는 스냅샷이므로 lock 불필요
    """

    def __init__(self, sandbox: bool = False):
        self._sandbox  = sandbox
        self._ws_base  = _WS_DEMO_URL if sandbox else _WS_BASE_URL

        # {심볼(소문자, 슬래시 제거): deque} — 예: "btcusdt" → deque
        self._queues: Dict[str, deque] = {}

        # {심볼: 마지막 업데이트 UTC timestamp}
        self._last_update: Dict[str, float] = {}

        # 스트리밍 활성 여부 플래그
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── 공개 인터페이스 ──────────────────────────────────────────────────

    def start(self, symbols: list[str] = None) -> None:
        """
        WebSocket 스트리밍을 백그라운드 daemon 스레드로 시작합니다.

        Args:
            symbols: 구독할 심볼 목록 (예: ["BTC/USDT"]). None이면 시작만 준비.
        """
        if self._running:
            logger.debug("⚡ [OhlcvStream] 이미 실행 중 — start() 무시")
            return

        self._running = True
        self._symbols = [self._normalize(s) for s in (symbols or ["btcusdt"])]

        # 각 심볼 큐 초기화
        for sym in self._symbols:
            if sym not in self._queues:
                self._queues[sym] = deque(maxlen=_CANDLE_QUEUE_MAXLEN)

        self._thread = threading.Thread(
            target=self._run_event_loop,
            name="ohlcv-ws-stream",
            daemon=True,  # 메인 프로세스 종료 시 자동 정리
        )
        self._thread.start()
        logger.info(
            "⚡ [OhlcvStream] WebSocket 스트림 스레드 시작 (symbols=%s, sandbox=%s)",
            self._symbols, self._sandbox,
        )

    def stop(self) -> None:
        """WebSocket 스트리밍을 안전하게 종료합니다."""
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        logger.info("🛑 [OhlcvStream] 스트림 종료 요청")

    def get_latest_df(
        self,
        symbol: str,
        min_candles: int = 60,
    ) -> Optional[pd.DataFrame]:
        """
        인메모리 큐에서 최신 OHLCV DataFrame을 반환합니다 (0ms 근접).

        [방어 로직]
        - 큐가 비어있거나 min_candles 미달 시 None 반환 → 호출자가 REST Fallback 수행
        - 캔들 복사본(list snapshot)을 반환하여 데이터 일관성 보장

        Args:
            symbol:      심볼 (예: "BTC/USDT")
            min_candles: 최소 필요 캔들 수 (기본: 60)

        Returns:
            DataFrame (컬럼: timestamp_ms, open, high, low, close, volume)
            또는 None (데이터 부족)
        """
        key = self._normalize(symbol)
        q = self._queues.get(key)

        if q is None or len(q) < min_candles:
            logger.debug(
                "⏸️ [OhlcvStream] 큐 데이터 부족 (symbol=%s, 현재=%d, 필요=%d) → REST Fallback 필요",
                symbol, len(q) if q else 0, min_candles,
            )
            return None

        # GIL 보호 하에 원자적 스냅샷 — lock 불필요
        snapshot = list(q)

        df = pd.DataFrame(
            snapshot,
            columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
        )
        # 타임스탬프 오름차순 정렬 + 중복 제거.
        # WebSocket 확정 캔들과 REST seed_from_rest()가 동일 ts를 각각 append할 수 있어
        # 같은 캔들이 2행으로 들어가면 compute_all_features()의 지표가 왜곡된다.
        # 같은 ts는 가장 마지막(최신) 값만 남겨 1행으로 정규화한다.
        df = df.sort_values("timestamp_ms", ascending=True)
        df = df.drop_duplicates(subset="timestamp_ms", keep="last").reset_index(drop=True)
        return df

    def is_alive(self, symbol: str, max_stale_sec: float = 90.0) -> bool:
        """
        스트림이 살아있고 최신 데이터가 max_stale_sec 이내에 수신됐는지 확인합니다.

        Args:
            symbol:       확인할 심볼
            max_stale_sec: 최대 허용 데이터 유효 기간 (초, 기본 90초 = 1.5분 캔들)

        Returns:
            True → 스트림 정상, False → 재연결 필요 또는 스트림 미시작
        """
        key = self._normalize(symbol)
        last = self._last_update.get(key, 0.0)
        return self._running and (time.monotonic() - last) < max_stale_sec

    def queue_size(self, symbol: str) -> int:
        """현재 큐에 쌓인 캔들 수를 반환합니다."""
        key = self._normalize(symbol)
        q = self._queues.get(key)
        return len(q) if q else 0

    # ── 내부 구현 ────────────────────────────────────────────────────────

    @staticmethod
    def _normalize(symbol: str) -> str:
        """'BTC/USDT' → 'btcusdt' (Binance WS 스트림 이름 형식)"""
        return symbol.replace("/", "").replace("-", "").lower()

    def _run_event_loop(self) -> None:
        """전용 스레드에서 asyncio 이벤트루프를 구동합니다."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._stream_all())
        except Exception as exc:
            logger.error("❌ [OhlcvStream] 이벤트루프 치명적 예외: %s", exc, exc_info=True)
        finally:
            self._loop.close()
            logger.info("🛑 [OhlcvStream] 이벤트루프 종료")

    async def _stream_all(self) -> None:
        """모든 심볼의 WebSocket 스트림을 동시에 구동합니다."""
        tasks = [
            asyncio.create_task(self._stream_symbol(sym))
            for sym in self._symbols
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_symbol(self, symbol_key: str) -> None:
        """
        단일 심볼 WebSocket 스트림 구독 루프 (Exponential Backoff 재연결).

        스트림 이름: <symbol>@kline_1m
        예: "btcusdt@kline_1m" → BTC/USDT 1분 캔들 실시간 수신
        """
        wait_sec = _RECONNECT_MIN_SEC
        stream_name = f"{symbol_key}@kline_1m"
        ws_url = f"{self._ws_base}/{stream_name}"

        while self._running:
            try:
                await self._connect_and_receive(symbol_key, ws_url)
                # 정상 종료된 경우 (stop() 호출)
                if not self._running:
                    break
                # 비정상 종료 → Backoff 후 재연결
                logger.warning(
                    "⚠️ [OhlcvStream] %s 연결 종료 — %.0f초 후 재연결",
                    stream_name, wait_sec,
                )
            except Exception as exc:
                logger.error(
                    "❌ [OhlcvStream] %s 연결 오류 — %.0f초 후 재연결: %s",
                    stream_name, wait_sec, exc,
                )

            await asyncio.sleep(wait_sec)
            # Exponential Backoff: 1s → 2s → 4s → ... → 60s 상한
            wait_sec = min(wait_sec * _RECONNECT_BACKOFF, _RECONNECT_MAX_SEC)

        logger.info("🛑 [OhlcvStream] %s 스트림 루프 종료", stream_name)

    async def _connect_and_receive(self, symbol_key: str, ws_url: str) -> None:
        """
        WebSocket 연결 후 kline 메시지를 수신하여 큐에 적재합니다.

        [메시지 구조 — Binance kline 스트림]
        {
          "e": "kline",
          "k": {
            "t": 1234567890000,  # 캔들 시작 타임스탬프 (ms)
            "o": "9000.00",      # 시가
            "h": "9100.00",      # 고가
            "l": "8900.00",      # 저가
            "c": "9050.00",      # 종가
            "v": "100.0",        # 거래량
            "x": true/false      # 캔들 마감 여부 (true=확정 캔들)
          }
        }

        마감된 캔들(x=true)만 큐에 적재합니다.
        미마감 캔들은 '현재가 참조'용으로 별도 레지스터에 보관합니다.
        """
        # websockets 라이브러리 안전 임포트 (선택적 의존성)
        try:
            import websockets
        except ImportError:
            logger.error(
                "❌ [OhlcvStream] 'websockets' 라이브러리 미설치. "
                "pip install websockets 를 실행하세요."
            )
            return

        logger.info("🔗 [OhlcvStream] WebSocket 연결 시도: %s", ws_url)
        async with websockets.connect(
            ws_url,
            ping_interval=_PING_INTERVAL_SEC,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            logger.info("✅ [OhlcvStream] WebSocket 연결 성공: %s", ws_url)
            # 연결 성공 → Backoff 리셋은 상위 루프에서 처리
            async for raw_msg in ws:
                if not self._running:
                    break
                self._handle_kline_message(symbol_key, raw_msg)

    def _handle_kline_message(self, symbol_key: str, raw_msg: str) -> None:
        """
        수신된 WebSocket kline 메시지를 파싱하여 큐에 적재합니다.

        [방어적 파싱]
        - JSON 파싱 실패: 무시 (로그만 기록)
        - 필드 누락: 무시 (기형 메시지 방어)
        - 미마감 캔들(x=false): 큐 적재 생략 (확정 데이터만 보관)
        """
        try:
            msg = json.loads(raw_msg)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("⚠️ [OhlcvStream] JSON 파싱 실패: %s", exc)
            return

        # kline 이벤트 필드 추출
        kline = msg.get("k", {})
        if not kline:
            return

        # 확정 캔들(x=true)만 큐에 적재
        # 미마감 캔들은 지표 연산 왜곡을 유발하므로 제외
        is_closed = kline.get("x", False)

        # 현재 미마감 캔들의 실시간 가격을 별도 레지스터에 보관
        # (analyze_and_trade에서 현재가 참조용으로 활용 가능)
        try:
            ts_ms  = int(kline["t"])
            o_val  = float(kline["o"])
            h_val  = float(kline["h"])
            l_val  = float(kline["l"])
            c_val  = float(kline["c"])
            v_val  = float(kline["v"])
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("⚠️ [OhlcvStream] kline 필드 파싱 실패: %s", exc)
            return

        # NaN/Inf 방어
        vals = (o_val, h_val, l_val, c_val, v_val)
        if any(math.isnan(v) or math.isinf(v) for v in vals):
            logger.warning("⚠️ [OhlcvStream] NaN/Inf 값 감지 — 해당 캔들 무시")
            return

        # 확정 캔들만 영구 큐에 적재
        if is_closed:
            q = self._queues.get(symbol_key)
            if q is not None:
                q.append([ts_ms, o_val, h_val, l_val, c_val, v_val])
                self._last_update[symbol_key] = time.monotonic()
                logger.debug(
                    "📥 [OhlcvStream] %s 캔들 적재 — ts=%d, c=%.2f, 큐=%d/%d",
                    symbol_key, ts_ms, c_val, len(q), _CANDLE_QUEUE_MAXLEN,
                )

    def seed_from_rest(
        self,
        symbol: str,
        ohlcv_list: list,
    ) -> int:
        """
        REST API로 조회한 OHLCV 데이터를 큐에 초기 적재합니다 (Cold Start 지원).

        WebSocket 연결 후 큐가 비어있는 초기 상태에서,
        REST로 가져온 과거 500봉을 큐에 먼저 채워 warm-up 시간을 최소화합니다.

        Args:
            symbol:     심볼 (예: "BTC/USDT")
            ohlcv_list: ccxt fetch_ohlcv() 반환값
                        [[ts_ms, open, high, low, close, volume], ...]

        Returns:
            적재된 캔들 수
        """
        key = self._normalize(symbol)
        if key not in self._queues:
            self._queues[key] = deque(maxlen=_CANDLE_QUEUE_MAXLEN)

        q = self._queues[key]
        count = 0
        for row in sorted(ohlcv_list, key=lambda x: x[0]):
            try:
                ts_ms = int(row[0])
                o, h, l, c, v = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
                if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c, v)):
                    continue
                q.append([ts_ms, o, h, l, c, v])
                count += 1
            except (IndexError, ValueError, TypeError):
                continue

        if count:
            self._last_update[key] = time.monotonic()

        logger.info(
            "🌱 [OhlcvStream] %s REST seed 완료 — %d봉 적재 (큐=%d/%d)",
            symbol, count, len(q), _CANDLE_QUEUE_MAXLEN,
        )
        return count


# ── 모듈 레벨 싱글턴 인스턴스 ───────────────────────────────────────────
# Celery 워커 임포트 시 1회 초기화됩니다.
# tasks.py에서 이 인스턴스를 import하여 사용합니다.
from core.config import get_settings as _get_settings
_settings = _get_settings()
ohlcv_stream_manager = OhlcvStreamManager(sandbox=_settings.exchange_sandbox)
