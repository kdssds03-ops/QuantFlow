# ────────────────────────────────────────────────────────────
# QuantFlow — Multi-stage Dockerfile
# ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

# 시스템 의존성 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl && \
    rm -rf /var/lib/apt/lists/*

# 보안 베스트 프랙티스: 비특권 유저 및 그룹 생성
RUN addgroup --system quantflow && adduser --system --ingroup quantflow quantflow

WORKDIR /opt/quantflow

# 의존성 먼저 복사 → Docker 캐시 극대화
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스코드 복사 및 작업 디렉토리 전체 소유권 부여
COPY --chown=quantflow:quantflow . .
RUN chown -R quantflow:quantflow /opt/quantflow

ENV PYTHONPATH=/opt/quantflow
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 컨테이너 실행 권한을 비특권 유저로 전환 (SecurityWarning 방어)
USER quantflow

# ── API 서버 (기본 CMD) ──
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
