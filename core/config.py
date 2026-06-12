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
    # 자동으로 sys_status=PAUSED로 신규 진입을 동결한다(보유 포지션 보호 가드는 유지,
    # 다음 KST 거래일 자동 재개). 0 = 비활성(기본).
    # 예: 0.05 = -5% 도달 시 당일 신규 진입 중단. 실전에서는 0.03~0.10 설정 강력 권장.
    max_daily_loss_pct: float = 0.0

    # 자본 방화벽: 단일 심볼 1회 진입 노셔널 ≤ 가용마진 × 이 비율 (초과 시 주문 거부).
    # 반드시 risk_factor(변동성타겟 ON이면 × vol_scale_max)보다 크게 둘 것 —
    # 더 작으면 모든 신규 진입이 거부되어 봇이 무거래 상태가 된다 (기동 시 교차 검증 로그 있음).
    max_capital_per_symbol_pct: float = 0.60

    # ── 변동성 타게팅 (동적 사이징) ──────────────────────────────
    # 최근 변동성이 장기평균보다 높으면 진입 사이징을 줄이고, 낮으면 키운다.
    # 평균 사이징은 risk_factor에 유지되며, 변동성에 따라 조절만 한다.
    # ⚠️ 2026-06-12 라이브 충실 재현(진입 시 1회 샘플링, scripts/risk_factor_scenarios.py):
    #    IS Sharpe 0.75→0.96 개선이나 OOS 0.70→0.51 악화 — 'IS/OOS 동시 우위' 채택 기준
    #    미달로 기본 OFF. (과거 "0.68→0.81 양 구간 개선" 근거는 매봉 무비용 리밸런싱
    #    모델이라 라이브의 진입 시 1회 샘플링 구현과 다른 메커니즘이었음)
    # effective_risk = risk_factor * clip(anchor_vol/realized_vol, scale_min, scale_max)
    vol_target_enabled: bool = False          # True 시 동적 사이징 활성
    vol_realized_4h_bars: int = 30            # 실현변동성 추정 4h봉 수 (≈5일)
    vol_anchor_halflife_days: float = 90.0    # 장기 변동성 앵커 EMA 반감기(일)
    vol_scale_min: float = 0.5                # 사이징 축소 하한 (0.5배)
    vol_scale_max: float = 2.0                # 사이징 확대 상한 (2.0배)

    # 시스템 헬스체크 알림 정책: True면 이상(⚠️/❌)일 때만 텔레그램 발송(정상은 로그만).
    # False면 매 점검(12h)마다 정상 리포트도 발송(데드맨 스위치 효과).
    healthcheck_alert_only: bool = True

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
