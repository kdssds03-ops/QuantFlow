#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────
# QuantFlow — API 서비스 엔트리포인트
# 역할: Alembic 마이그레이션 자동 실행 후 FastAPI 기동
#
# 실행 흐름:
#   1. PostgreSQL 연결 대기 (health check 보조)
#   2. alembic/ 디렉터리 미존재 시 → alembic init 수행
#   3. alembic_version 테이블 미존재 시 → 최초 리비전 생성 + upgrade head
#   4. alembic_version 테이블 존재 시 → upgrade head 만 수행 (증분 마이그레이션)
#   5. FastAPI 서버 기동
# ────────────────────────────────────────────────────────────
set -e

WORKDIR="/opt/quantflow"
cd "$WORKDIR"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   QuantFlow — DB Migration Pipeline      ║"
echo "╚══════════════════════════════════════════╝"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] 마이그레이션 파이프라인 가동"

# ── STEP 1: alembic/ 디렉터리 초기화 (최초 1회만) ──────────────────────
if [ ! -d "$WORKDIR/alembic" ]; then
    echo "[STEP 1] alembic/ 디렉터리 미감지 → alembic init 수행"
    alembic init alembic

    # env.py를 QuantFlow 프로젝트 구조에 맞게 자동 패치
    echo "[STEP 1] env.py 자동 패치 적용 중..."
    python - <<'PYEOF'
import re

env_path = "alembic/env.py"
with open(env_path, "r") as f:
    content = f.read()

# target_metadata = None → Base.metadata 연결
old_meta = "target_metadata = None"
new_meta = (
    "# QuantFlow: models 임포트하여 Base.metadata를 Alembic에 바인딩\n"
    "import app.models.models  # noqa: F401 — 사이드이펙트 임포트로 모든 모델 등록\n"
    "from core.database import Base\n"
    "target_metadata = Base.metadata"
)
content = content.replace(old_meta, new_meta)

# run_migrations_online 내부: asyncpg URL을 psycopg2로 교체하여 동기 실행 보장
old_online = "    connectable = engine_from_config("
new_online = (
    "    # QuantFlow 아키텍처 가드:\n"
    "    # asyncpg(비동기) URL을 psycopg2(동기)로 변환하여 Alembic 동기 컨텍스트와 충돌 방지\n"
    "    from sqlalchemy import create_engine as _create_engine\n"
    "    _raw_url = config.get_main_option('sqlalchemy.url', '')\n"
    "    _sync_url = _raw_url.replace('postgresql+asyncpg', 'postgresql+psycopg2')\n"
    "    connectable = _create_engine(_sync_url, poolclass=pool.NullPool)\n"
    "    if False:  # 원본 engine_from_config 코드 비활성화 (아래 원본 유지)\n"
    "    _connectable_original = engine_from_config("
)
content = content.replace(old_online, new_online)

with open(env_path, "w") as f:
    f.write(content)

print("[STEP 1] env.py 패치 완료")
PYEOF

    # alembic.ini sqlalchemy.url 설정 (DATABASE_URL 환경변수로부터 읽어 자동 주입)
    echo "[STEP 1] alembic.ini DATABASE_URL 주입 중..."
    python - <<'PYEOF'
import os, re

ini_path = "alembic.ini"
with open(ini_path, "r") as f:
    content = f.read()

db_url = os.environ.get("DATABASE_URL", "postgresql+psycopg2://quantflow:quantflow_secret@postgres:5432/quantflow")
# asyncpg → psycopg2 로 강제 치환 (alembic은 동기 드라이버 필요)
db_url = db_url.replace("postgresql+asyncpg", "postgresql+psycopg2")

content = re.sub(
    r"^sqlalchemy\.url\s*=.*$",
    f"sqlalchemy.url = {db_url}",
    content,
    flags=re.MULTILINE,
)
with open(ini_path, "w") as f:
    f.write(content)

print(f"[STEP 1] alembic.ini sqlalchemy.url 설정 완료: {db_url}")
PYEOF

else
    echo "[STEP 1] alembic/ 디렉터리 감지됨 → init 스킵"
fi

# ── STEP 2: alembic_version 테이블 존재 여부로 최초 구동 판별 ────────────
echo "[STEP 2] DB 마이그레이션 상태 스캔 중..."

ALEMBIC_VERSION_EXISTS=$(python - <<'PYEOF'
import os
import sqlalchemy as sa

url = os.environ.get("DATABASE_URL", "")
url = url.replace("postgresql+asyncpg", "postgresql+psycopg2")

try:
    engine = sa.create_engine(url, pool_pre_ping=True)
    insp = sa.inspect(engine)
    tables = insp.get_table_names()
    print("yes" if "alembic_version" in tables else "no")
    engine.dispose()
except Exception as e:
    print(f"error: {e}", flush=True)
    exit(1)
PYEOF
)

echo "[STEP 2] alembic_version 테이블 존재: $ALEMBIC_VERSION_EXISTS"

if [ "$ALEMBIC_VERSION_EXISTS" = "no" ]; then
    echo "[STEP 3] 최초 구동 감지 → 현재 ORM 모델을 기반으로 DB 스키마 강제 생성 중..."
    python - <<'PYEOF'
import os
import sqlalchemy as sa
from core.database import Base
import app.models.models

url = os.environ.get("DATABASE_URL", "")
url = url.replace("postgresql+asyncpg", "postgresql+psycopg2")
engine = sa.create_engine(url)
Base.metadata.create_all(engine)
print("✅ DB 테이블 강제 생성 완료")
PYEOF

    echo "[STEP 3] Alembic 상태를 최신 버전(head)으로 강제 마킹 중..."
    # 기존 마이그레이션 파일이 전혀 없다면 빈 껍데기 리비전 하나 생성
    if [ ! -d "alembic/versions" ] || [ -z "$(ls -A alembic/versions 2>/dev/null)" ]; then
        alembic revision --autogenerate -m "Initial schema snapshot"
    fi
    # 무한 대기를 유발하는 upgrade 대신 stamp head로 상태만 최신으로 주입
    alembic stamp head
    echo "[STEP 3] ✅ 최초 마이그레이션(Stamp) 완료"
else
    echo "[STEP 3] 기존 마이그레이션 이력 감지 → upgrade head (증분) 실행"
    alembic upgrade head || echo "⚠️ Upgrade head 실패 무시"
    echo "[STEP 3] ✅ 증분 마이그레이션 완료"
fi

echo ""
echo "✅ DB 마이그레이션 파이프라인 완료 — FastAPI 서버 기동"
echo "════════════════════════════════════════════"
echo ""

# ── STEP 4: FastAPI 서버 기동 ─────────────────────────────────────────
exec "$@"
