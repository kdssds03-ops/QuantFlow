"""
app.main — FastAPI 애플리케이션 엔트리포인트
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import get_settings
from core.database import engine, Base
from core.redis import redis_client
from core.time_sync import check_ntp_drift
from app.api import router as api_router

settings = get_settings()

# ── 로깅 설정 ────────────────────────────────
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Lifespan (startup / shutdown) ────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 리소스 관리"""
    logger.info(f"🚀 {settings.app_name} 시작 (env={settings.app_env})")

    # DB 테이블 자동 생성 (개발 환경 전용, 프로덕션은 Alembic 사용)
    if settings.debug:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("📦 DB 테이블 생성 완료 (debug 모드)")

    # NTP 시간 차이 확인
    import asyncio
    drift = await asyncio.to_thread(check_ntp_drift)
    if drift is not None:
        logger.info(f"⏱  NTP drift: {drift:.1f}ms")
    else:
        logger.warning("⏱  NTP drift: 측정 불가 (모든 NTP 서버 응답 없음)")

    yield

    # Shutdown
    await redis_client.aclose()
    await engine.dispose()
    logger.info(f"👋 {settings.app_name} 종료")


# ── FastAPI 인스턴스 ─────────────────────────
app = FastAPI(
    title=settings.app_name,
    description="24시간 자동매매 시스템 API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────
# 허용 오리진은 settings(.env CORS_ALLOW_ORIGINS)에서 로드.
# 와일드카드("*") 사용 시 브라우저 사양상 credentials를 함께 허용할 수 없으므로,
# 오리진이 명시적으로 제한된 경우에만 allow_credentials=True 로 활성화한다.
_cors_origins = settings.cors_origins_list
_allow_credentials = _cors_origins != ["*"]
if _cors_origins == ["*"]:
    logger.warning(
        "⚠️ CORS가 전체 오리진('*')을 허용하도록 설정되어 있습니다. "
        "프로덕션에서는 .env의 CORS_ALLOW_ORIGINS를 특정 도메인으로 제한하세요."
    )
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 라우터 등록 ──────────────────────────────
app.include_router(api_router, prefix="/api/v1")


# ── 헬스체크 ─────────────────────────────────
# Docker Compose healthcheck: `curl -sf http://localhost:8000/health`
# worker / beat 서비스는 이 엔드포인트가 200을 반환해야만 기동됩니다.
@app.get("/health", tags=["System"])
async def health_check():
    """컨테이너 헬스체크 및 시스템 상태 확인용 엔드포인트"""
    return {"status": "ok", "service": settings.app_name}
