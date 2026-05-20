#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────
# QuantFlow — Worker / Beat 공용 엔트리포인트
# 역할: DB 마이그레이션 완료 대기 후 Celery 기동
#
# 마이그레이션 책임은 api 서비스(entrypoint-api.sh)에 있음.
# Worker/Beat는 alembic_version 테이블이 생길 때까지 대기 후 기동.
# ────────────────────────────────────────────────────────────
set -e

echo "[QuantFlow Worker] 🕐 $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "[QuantFlow Worker] DB 마이그레이션 완료 대기 중..."

# alembic_version 테이블이 존재할 때까지 최대 60초 대기
MAX_WAIT=60
ELAPSED=0
INTERVAL=3

until python - <<'PYEOF'
import os, sys
import sqlalchemy as sa

url = os.environ.get("DATABASE_URL", "").replace("postgresql+asyncpg", "postgresql+psycopg2")
try:
    engine = sa.create_engine(url, pool_pre_ping=True)
    insp = sa.inspect(engine)
    tables = insp.get_table_names()
    engine.dispose()
    if "alembic_version" in tables:
        sys.exit(0)   # 마이그레이션 완료 → 대기 종료
    else:
        sys.exit(1)   # 아직 미완료 → 대기 계속
except Exception:
    sys.exit(1)
PYEOF
do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo "[QuantFlow Worker] ❌ DB 마이그레이션 대기 타임아웃 (${MAX_WAIT}s). 강제 진행."
        break
    fi
    echo "[QuantFlow Worker] ⏳ 마이그레이션 대기 중... (${ELAPSED}s / ${MAX_WAIT}s)"
    sleep "$INTERVAL"
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo "[QuantFlow Worker] ✅ DB 준비 완료 — Celery 기동"
exec "$@"
