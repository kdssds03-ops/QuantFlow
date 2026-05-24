"""
core.exchange — ccxt 기반 거래소 연결
여러 거래소를 통합 인터페이스로 관리

Phase 5 (Timezone Fix):
  - fetch_ohlcv_utc() 래퍼: Binance OHLCV timestamp가 UTC milliseconds임을 검증/보장
  - 모든 타임스탬프를 UTC timezone-aware datetime으로 정규화하는 헬퍼 제공
"""

import logging
from datetime import datetime, timezone
from typing import List

import ccxt

from core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def get_exchange() -> ccxt.Exchange:
    """
    ccxt Exchange 인스턴스 생성.
    .env의 EXCHANGE_NAME에 따라 동적으로 거래소 선택.
    EXCHANGE_SANDBOX=true 일 때 Binance Demo(demo-fapi.binance.com)로 자동 전환.

    ── sapi 에러 완전 차단 전략 (3중 방어) ────────────────────────────────
    ccxt load_markets() 내부 호출 구조:
        ① fetch_currencies() → sapi/v1/capital/config/getall  [Demo 서버 없음 → 403]
        ② fetch_markets()    → spot publicGetExchangeInfo      [Demo 서버 없음 → 403]
                             → sapiGetMarginAllPairs           [Demo 서버 없음 → 403]

    방어 레이어:
        [1] ccxt.binanceusdm 사용
            → fetch_markets()가 fapiPublicGetExchangeInfo만 호출 (spot/margin 제거)
            → 단, fetch_currencies()는 부모 binance 클래스에서 상속되어 여전히 존재

        [2] has['fetchCurrencies'] = False
            → load_markets() 내부 조건:
              `if self.has.get('fetchCurrencies') is True: currencies = self.fetch_currencies()`
              False로 설정 시 fetch_currencies() 호출 자체를 건너뜀

        [3] fetch_currencies monkey-patch → 빈 dict 반환
            → [2]를 우회하는 경우에 대한 이중 안전망

        [4] URL: fapi.binance.com → demo-fapi.binance.com 도메인만 교체
            → 경로(/fapi/v1) 유지로 올바른 엔드포인트 호출 보장
    """
    if settings.exchange_sandbox:
        # ── Binance Demo 모드 ───────────────────────────────────────────────
        # [1] binanceusdm: USDT 마진 선물 전용 클래스
        #     fetch_markets() → fapiPublicGetExchangeInfo 만 호출
        #     fetch_balance() → fapiPrivateGetAccount
        #     fetch_ohlcv()   → fapiPublicGetKlines
        exchange = ccxt.binanceusdm(
            {
                "apiKey": settings.exchange_api_key,
                "secret": settings.exchange_api_secret,
                "enableRateLimit": True,
                "options": {
                    "adjustForTimeDifference": True,
                },
            }
        )

        # 구형 sandbox_mode(testnet) 비활성화
        exchange.set_sandbox_mode(False)

        # [2] fetch_currencies 비활성화
        #     binanceusdm도 binance 상속으로 fetch_currencies를 갖고 있음
        #     → has['fetchCurrencies'] = False 로 load_markets() 내 호출 차단
        exchange.has["fetchCurrencies"] = False

        # [3] monkey-patch: [2]를 우회하는 경우에 대한 이중 안전망
        exchange.fetch_currencies = lambda params={}: {}

        # [4] fapi URL 도메인만 교체 (경로 /fapi/v1 유지 필수)
        for api_type, url in list(exchange.urls["api"].items()):
            if isinstance(url, str) and "fapi.binance.com" in url:
                exchange.urls["api"][api_type] = url.replace(
                    "fapi.binance.com", "demo-fapi.binance.com"
                )

        logger.info(
            "🟡 [BINANCE-USDM] 신형 데모 모드 활성화\n"
            "   [1] binanceusdm → fetch_markets()가 fapi만 호출\n"
            "   [2] has['fetchCurrencies']=False → load_markets 내 currencies 호출 차단\n"
            "   [3] fetch_currencies monkey-patch → 빈 dict 반환 (이중 안전망)\n"
            "   [4] fapi.binance.com → demo-fapi.binance.com (경로 유지)"
        )

    else:
        # ── Mainnet 실전 모드 ───────────────────────────────────────────────
        exchange = ccxt.binance(
            {
                "apiKey": settings.exchange_api_key,
                "secret": settings.exchange_api_secret,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "spot",
                    "adjustForTimeDifference": True,
                },
            }
        )
        logger.info("🟢 [BINANCE] 실제 거래소(Mainnet) 모드 활성화")

    return exchange


def ms_to_utc(ts_ms: int) -> datetime:
    """
    밀리초 Unix 타임스탬프를 UTC timezone-aware datetime으로 변환합니다.

    ── 타임존 버그 방어 원리 ──────────────────────────────────────────────────
    Binance/demo-fapi 서버가 반환하는 OHLCV timestamp는 항상 UTC milliseconds.
    Python의 datetime.fromtimestamp()는 OS 로컬 시간대(KST = UTC+9)를 기준으로
    해석하므로, tz=timezone.utc를 명시하지 않으면 9시간 오프셋이 발생합니다.

    이 함수는 tz=timezone.utc를 강제 적용하여 KST/UTC 혼재를 원천 차단합니다.

    Args:
        ts_ms: 밀리초 Unix 타임스탬프 (Binance OHLCV 첫 번째 원소)

    Returns:
        timezone-aware UTC datetime
    """
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)


def fetch_ohlcv_utc(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str = "1m",
    limit: int = 500,
) -> List[list]:
    """
    ccxt fetch_ohlcv() 래퍼 — UTC timestamp 보장 버전.

    ── 해결하는 문제 ──────────────────────────────────────────────────────────
    봇 내부에서 Binance 데모 서버(demo-fapi.binance.com)가 반환하는 OHLCV
    timestamp는 UTC milliseconds이지만, 로컬 시스템이 KST(UTC+9)로 설정된
    환경에서 naive datetime으로 처리하면 9시간 불일치가 발생합니다.

    이 래퍼는:
      1. fetch_ohlcv() 결과를 그대로 반환합니다 (ccxt가 이미 UTC ms를 반환)
      2. timestamp_ms 기준 오름차순 정렬을 보장합니다
      3. 향후 정규화가 필요한 경우 이 함수에서 일괄 처리할 수 있습니다

    Args:
        exchange: get_exchange()로 생성된 ccxt.Exchange 인스턴스
        symbol:   거래 심볼 (예: "BTC/USDT")
        timeframe: 캔들 타임프레임 (기본: "1m")
        limit:    요청 캔들 수 (기본: 500 — EMA50 warm-up 완전 보장)

    Returns:
        [[timestamp_ms_utc, open, high, low, close, volume], ...] — 오름차순 정렬 보장
    """
    ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
    if not ohlcv:
        return ohlcv

    # timestamp_ms 기준 오름차순 정렬 — 타임존 혼재로 인한 순서 역전 방지
    ohlcv_sorted = sorted(ohlcv, key=lambda x: x[0])

    logger.debug(
        "📊 [fetch_ohlcv_utc] %s %s %d봉 수신 — "
        "시작: %s UTC, 종료: %s UTC",
        symbol, timeframe, len(ohlcv_sorted),
        ms_to_utc(ohlcv_sorted[0][0]).strftime("%Y-%m-%dT%H:%M:%S"),
        ms_to_utc(ohlcv_sorted[-1][0]).strftime("%Y-%m-%dT%H:%M:%S"),
    )

    return ohlcv_sorted
