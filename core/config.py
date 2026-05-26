"""
core.config — 환경변수 기반 글로벌 설정
pydantic-settings를 통해 .env 파일에서 자동 로드
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """모든 환경변수를 Pydantic 모델로 관리"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── App ──────────────────────────────────
    app_name: str = "QuantFlow"
    app_env: str = "development"
    debug: bool = True
    secret_key: str = "change-me"
    tz: str = "Asia/Seoul"

    # ── PostgreSQL ───────────────────────────
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "quantflow"
    postgres_password: str = "quantflow"
    postgres_db: str = "quantflow"
    database_url: str = "postgresql+asyncpg://quantflow:quantflow@postgres:5432/quantflow"

    # ── Redis ────────────────────────────────
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_url: str = "redis://redis:6379/0"

    # ── Celery ───────────────────────────────
    celery_broker_url: str = "redis://redis:6379/0"
    celery_result_backend: str = "redis://redis:6379/1"

    # ── Exchange ─────────────────────────────
    exchange_api_key: str = ""
    exchange_api_secret: str = ""
    exchange_name: str = "binance"
    exchange_sandbox: bool = False  # True → 테스트넷(샌드박스) 모드

    # ── Logging ──────────────────────────────
    log_level: str = "INFO"

    # ── Telegram ──────────────────────────────
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def sync_database_url(self) -> str:
        """Alembic 등 동기 드라이버용 URL"""
        return self.database_url.replace("+asyncpg", "+psycopg2")


@lru_cache()
def get_settings() -> Settings:
    """싱글턴 Settings 인스턴스 반환"""
    return Settings()
