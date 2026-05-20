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
    drift = check_ntp_drift()
    logger.info(f"⏱  NTP drift: {drift:.1f}ms")

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 프로덕션에서는 특정 도메인으로 제한
    allow_credentials=True,
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
