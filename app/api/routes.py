"""
app.api.routes — 시스템 상태 확인 엔드포인트
"""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings
from core.database import get_db
from core.redis import get_redis
from core.time_sync import check_ntp_drift, get_timestamp_ms

router = APIRouter()
settings = get_settings()


@router.get("/health")
async def health_check(
    db: AsyncSession = Depends(get_db),
):
    """
    시스템 헬스 체크
    - DB 연결 상태
    - Redis 연결 상태
    - NTP 시간 drift
    """
    # DB 확인
    try:
        await db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {e}"

    # Redis 확인
    try:
        redis = await get_redis()
        await redis.ping()
        redis_status = "connected"
    except Exception as e:
        redis_status = f"error: {e}"

    # NTP drift
    drift_ms = check_ntp_drift()

    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.app_env,
        "timestamp_ms": get_timestamp_ms(),
        "components": {
            "database": db_status,
            "redis": redis_status,
            "ntp_drift_ms": round(drift_ms, 1),
        },
    }


@router.get("/ping")
async def ping():
    """간단한 생존 확인"""
    return {"ping": "pong"}
