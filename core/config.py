"""
core.config — 환경변수 기반 글로벌 설정
pydantic-settings를 통해 .env 파일에서 자동 로드
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator


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
    debug: bool = False
    secret_key: str = "change-me"
    tz: str = "Asia/Seoul"

    # ── PostgreSQL ───────────────────────────
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "quantflow"
    postgres_password: str = "quantflow"
    postgres_db: str = "quantflow"
    database_url: str = ""

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

    # ── Strategy ─────────────────────────────
    # 1회 신규 진입에 투입할 가용 마진 비율 (사이징 = 위험/수익 손잡이).
    # 백테스트(4h 추세, 3년): 0.10 → 연 ~7%/MDD -8%, 0.30 → 연 ~19%/MDD -22%.
    # 권장 범위 0.05~0.50. 1.0 초과(레버리지 가중)는 파산 위험이 급증하므로 주의.
    risk_factor: float = 0.10

    # 일일 최대손실 서킷브레이커: 당일(KST) 시작자본 대비 손실이 이 비율에 도달하면
    # 자동으로 sys_status=PAUSED로 매매를 동결한다. 0 = 비활성(기본).
    # 예: 0.05 = -5% 도달 시 당일 매매 중단. 실전에서는 0.03~0.10 설정 강력 권장.
    max_daily_loss_pct: float = 0.0

    # ── Logging ──────────────────────────────
    log_level: str = "INFO"

    # ── CORS ─────────────────────────────────
    # 쉼표로 구분된 허용 오리진 목록. 기본값 "*"(전체 허용)은 개발 편의용이며,
    # 프로덕션에서는 .env에서 명시적 도메인으로 제한할 것을 강력 권장.
    # 예) CORS_ALLOW_ORIGINS="https://app.example.com,https://admin.example.com"
    cors_allow_origins: str = "*"

    # ── Telegram ──────────────────────────────
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    @model_validator(mode='after')
    def _assemble_database_url(self) -> 'Settings':
        """database_url이 명시적으로 설정되지 않으면 개별 필드로 자동 구성"""
        if not self.database_url:
            self.database_url = (
                f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        return self

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def sync_database_url(self) -> str:
        """Alembic 등 동기 드라이버용 URL"""
        return self.database_url.replace("+asyncpg", "+psycopg2")

    @property
    def cors_origins_list(self) -> list[str]:
        """쉼표 구분 CORS 오리진 문자열을 리스트로 파싱. '*'는 전체 허용."""
        raw = (self.cors_allow_origins or "").strip()
        if raw == "*" or not raw:
            return ["*"]
        return [origin.strip() for origin in raw.split(",") if origin.strip()]


@lru_cache()
def get_settings() -> Settings:
    """싱글턴 Settings 인스턴스 반환"""
    return Settings()
