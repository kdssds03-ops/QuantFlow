"""
core.database — Async SQLAlchemy 엔진 & 세션
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from core.config import get_settings

settings = get_settings()

# ── Async Engine ─────────────────────────────
# NullPool: Celery prefork 워커가 fork되거나 _run_async_safe()가 매번 새 이벤트
# 루프를 생성할 때, 풀에 캐시된 asyncpg 커넥션이 죽은 루프에 바인딩되어
# "attached to a different loop" / "Event loop is closed" 오류를 유발한다.
# 커넥션을 풀링하지 않고 매번 새로 열고 닫아 이벤트 루프 불일치를 원천 차단한다.
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    poolclass=NullPool,
)

# ── Session Factory ──────────────────────────
async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ── Declarative Base ─────────────────────────
class Base(DeclarativeBase):
    """모든 ORM 모델의 기본 클래스"""
    pass


# ── Dependency Injection ─────────────────────
async def get_db() -> AsyncSession:
    """FastAPI Depends()에서 사용하는 DB 세션 제공자"""
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
