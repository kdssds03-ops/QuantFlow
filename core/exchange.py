"""
core.exchange — ccxt 기반 거래소 연결
여러 거래소를 통합 인터페이스로 관리
"""

import logging

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
