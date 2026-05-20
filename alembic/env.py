"""
alembic/env.py — QuantFlow 맞춤형 Alembic 마이그레이션 환경 설정

핵심 아키텍처 결정:
  1. target_metadata = Base.metadata
     → SQLAlchemy의 DeclarativeBase로 정의된 모든 모델을 autogenerate가 감지
  2. asyncpg(비동기) URL → psycopg2(동기) URL 강제 변환
     → Alembic의 동기 실행 컨텍스트와 asyncpg 드라이버의 충돌을 원천 차단
  3. 환경변수 DATABASE_URL 우선, 없으면 alembic.ini sqlalchemy.url 폴백
"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlalchemy import create_engine

from alembic import context

# ── Alembic Config 객체 ──────────────────────────────────────────────────
config = context.config

# logging 설정 (alembic.ini의 [loggers] 섹션 적용)
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── QuantFlow 모델 메타데이터 바인딩 ────────────────────────────────────
# 반드시 모든 모델 모듈을 임포트해야 Base.metadata에 테이블이 등록됨
import app.models.models  # noqa: F401 — 사이드이펙트 임포트 (MarketData, TradeHistory 등록)
from core.database import Base

target_metadata = Base.metadata


# ── asyncpg → psycopg2 URL 변환 헬퍼 ────────────────────────────────────
def _get_sync_url() -> str:
    """
    환경변수 DATABASE_URL을 우선 읽고, 없으면 alembic.ini의 sqlalchemy.url을 사용.
    어떤 경우든 asyncpg 드라이버 접두어를 psycopg2로 치환하여 동기 실행 보장.
    """
    # 환경변수 우선 (docker-compose에서 .env로 주입된 값)
    raw_url = os.environ.get("DATABASE_URL", "") or config.get_main_option("sqlalchemy.url", "")

    if not raw_url:
        raise ValueError(
            "[Alembic] DATABASE_URL 환경변수 또는 alembic.ini sqlalchemy.url이 설정되지 않았습니다."
        )

    # 비동기 드라이버 → 동기 드라이버로 강제 치환
    sync_url = raw_url.replace("postgresql+asyncpg", "postgresql+psycopg2")
    return sync_url


# ── 오프라인 마이그레이션 (SQL 스크립트 생성 모드) ──────────────────────
def run_migrations_offline() -> None:
    """
    DB 연결 없이 SQL 스크립트 파일을 생성하는 오프라인 모드.
    `alembic upgrade head --sql` 실행 시 진입.
    """
    url = _get_sync_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Enum 타입 등 PostgreSQL 방언 비교 시 스키마 비교 정확도 향상
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ── 온라인 마이그레이션 (실제 DB 연결 모드) ─────────────────────────────
def run_migrations_online() -> None:
    """
    실제 DB에 연결하여 마이그레이션을 수행하는 온라인 모드 (기본값).

    아키텍처 가드:
    - engine_from_config() 대신 create_engine()을 직접 호출하여
      asyncpg URL이 alembic.ini에서 그대로 흘러들어오는 경우도 psycopg2로 강제 치환.
    - pool.NullPool: 마이그레이션 완료 후 커넥션을 즉시 해제 (컨테이너 환경 안전)
    """
    sync_url = _get_sync_url()

    connectable = create_engine(
        sync_url,
        poolclass=pool.NullPool,  # 마이그레이션 후 커넥션 즉시 반환
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # 컬럼 타입 변경(Numeric precision 변경 등) 감지
            compare_type=True,
            # 서버 기본값 변경 감지
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# ── 진입점: offline / online 분기 ───────────────────────────────────────
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
