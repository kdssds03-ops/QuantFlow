"""
worker.celery_app — Celery 인스턴스 설정 & Beat 스케줄
"""

from celery import Celery
from celery.schedules import crontab

from core.config import get_settings

settings = get_settings()

# ── Celery 인스턴스 ──────────────────────────
celery_app = Celery(
    "quantflow",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

# ── 기본 설정 ─────────────────────────────────
celery_app.conf.update(
    # 직렬화
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=settings.tz,
    enable_utc=True,

    # 안정성
    task_acks_late=True,               # 완료 후 ACK → 워커 다운 시 재시도
    worker_prefetch_multiplier=1,      # 공정한 분배
    task_reject_on_worker_lost=True,   # 워커 비정상 종료 시 태스크 재큐잉
    task_track_started=True,           # STARTED 상태 추적

    # 결과 만료
    result_expires=3600,               # 1시간 후 결과 자동 삭제

    # 큐 라우팅
    task_routes={
        "worker.tasks.fetch_market_data_task": {"queue": "market_data"},
        "worker.tasks.analyze_and_trade":     {"queue": "trading"},
        "worker.tasks.generate_daily_report_task": {"queue": "default"},
        # [Deadlock 근본 차단] 텔레그램 리스너 전용 큐 분리
        # trading/market_data/default 워커와 슬롯 공유 완전 차단
        # → /status 명령은 매매 진행 여부 무관, 항상 즉시 슬롯 확보 보장
        "worker.tasks.telegram_command_listener_task": {"queue": "listener"},
    },
)

# ── 태스크 자동 검색 ─────────────────────────
celery_app.autodiscover_tasks(["worker"])

# ── Beat 스케줄 (주기적 태스크) ───────────────
#
#  crontab(minute="*")  →  매 분 0초에 실행 (00:00, 00:01, 00:02, ...)
#  schedule=60.0        →  마지막 실행 후 60초 간격 (시작 시각에 따라 drift 가능)
#
#  거래 시스템에서는 crontab 방식이 더 예측 가능하므로 crontab 사용.
#
# ── 거래 심볼 결정: 포트폴리오 모드면 다중 심볼 fan-out, 아니면 BTC 단일(기존) ──
_trade_symbols = (
    settings.portfolio_symbols_list if settings.portfolio_mode else ["BTC/USDT"]
) or ["BTC/USDT"]

_beat_trading_tasks = {}
for _sym in _trade_symbols:
    _slug = _sym.replace("/", "-").lower()
    # 매 1분 1m 캔들 수집
    _beat_trading_tasks[f"fetch-{_slug}-1m-candle"] = {
        "task": "worker.tasks.fetch_market_data_task",
        "schedule": crontab(minute="*"),
        "kwargs": {"symbol": _sym},
        "options": {"queue": "market_data"},
    }
    # 매 60초 시그널 분석 & 자동매매
    _beat_trading_tasks[f"analyze-and-trade-{_slug}"] = {
        "task": "worker.tasks.analyze_and_trade",
        "schedule": 60.0,
        "kwargs": {"symbol": _sym},
        "options": {"queue": "trading"},
    }

celery_app.conf.beat_schedule = {

    # ── 거래 심볼별 수집+매매 (단일 BTC 또는 포트폴리오 fan-out) ──
    **_beat_trading_tasks,

    # ── 매일 23:59(Asia/Seoul) 일간 결산 리포트 ────────
    "daily-report-2359": {
        "task": "worker.tasks.generate_daily_report_task",
        "schedule": crontab(hour=23, minute=59),
        "options": {"queue": "default"},
    },

    # ── 30초마다 텍레그램 명령어 수신 (전용 격리 큐) ────
    "telegram-command-listener": {
        "task": "worker.tasks.telegram_command_listener_task",
        "schedule": 30.0,
        # ⇒ listener 큐: 매매/데이터 태스크와 워커 슬롯 공유 원체 차단
        # ⇒ /status 명령은 매매 진행여부 무관하게 항상 즉시 응답 보장
        "options": {"queue": "listener"},
    },

    # ── 12시간마다 시스템 자가 헬스체크 → 텔레그램 발송 (00:05, 12:05 KST) ──
    #    Claude 앱·노트북 무관하게 서버 내부에서 24h 자가감시 (데드맨 스위치 역할)
    "system-healthcheck-12h": {
        "task": "worker.tasks.system_health_check_task",
        "schedule": crontab(hour="*/12", minute=5),
        "options": {"queue": "default"},
    },
}
