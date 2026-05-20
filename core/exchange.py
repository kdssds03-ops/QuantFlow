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
    EXCHANGE_SANDBOX=true 일 때 테스트넷(샌드박스)으로 자동 전환.
    """
    exchange_class = getattr(ccxt, settings.exchange_name)
    exchange = exchange_class(
        {
            "apiKey": settings.exchange_api_key,
            "secret": settings.exchange_api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",  # 'spot' | 'future' | 'swap'
            },
        }
    )

    if settings.exchange_sandbox:
        # ✅ 테스트넷 모드 활성화
        # set_sandbox_mode()는 내부적으로 API endpoint를 
        # Mainnet → Testnet URL로 교체합니다.
        exchange.set_sandbox_mode(True)
        logger.info(
            f"🟡 [{settings.exchange_name.upper()}] 샌드박스(테스트넷) 모드 활성화"
        )
    else:
        logger.info(
            f"🟢 [{settings.exchange_name.upper()}] 실제 거래소(Mainnet) 모드 활성화"
        )

    return exchange

