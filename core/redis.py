"""
core.redis — Redis 연결 관리
캐시, 실시간 시세 저장, 락(Lock) 등에 사용
"""

from redis.asyncio import Redis

from core.config import get_settings

settings = get_settings()

# ── Async Redis Client ───────────────────────
redis_client = Redis.from_url(
    settings.redis_url,
    decode_responses=True,
    max_connections=20,
)


async def get_redis() -> Redis:
    """FastAPI Depends()에서 사용하는 Redis 클라이언트"""
    return redis_client
