"""
core.database — Async SQLAlchemy 엔진 & 세션
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from core.config import get_settings

settings = get_settings()

# ── Async Engine ─────────────────────────────
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
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
        finally:
            await session.close()
