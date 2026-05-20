"""
scripts/backfill_data.py — ML 모델 학습용 대규모 과거 데이터 백필 스크립트

기능 요약:
  1. 바이낸스 BTC/USDT 1m 캔들 데이터를 ccxt로 페이지네이션 수집 (최근 30일 / ~43,200개)
  2. 수집된 전체 OHLCV를 하나의 DataFrame으로 합친 후 pandas-ta로 지표 일괄 계산
     - SMA(20), RSI(14), Bollinger Bands(20, 2)
  3. 1,000개 청크 단위로 PostgreSQL market_data 테이블에 벌크 Upsert

실행 방법 (프로젝트 루트 기준):
  # 도커 컨테이너 내부에서 직접 실행:
  docker compose exec worker python -m scripts.backfill_data

  # 또는 로컬에서 (DB/API 환경변수 설정 후):
  python -m scripts.backfill_data
"""

from __future__ import annotations

import logging
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import ccxt
import pandas as pd
import pandas_ta as ta
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

# ── 프로젝트 루트를 sys.path에 추가 (직접 실행 시 필요) ──────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.config import get_settings          # noqa: E402
from app.models.models import MarketData      # noqa: E402
from core.database import Base                # noqa: E402  ← Base.metadata 등록용

# ── 로거 설정 ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill_data")

# ── 상수 ──────────────────────────────────────────────────────────────────
SYMBOL        = "BTC/USDT"
TIMEFRAME     = "1m"
DAYS_BACK     = 30            # 수집 기간 (일)
API_LIMIT     = 1000          # ccxt 한 번 호출 Limit (바이낸스 최대)
SLEEP_SEC     = 0.5           # 호출 사이 대기 시간 (초) — HTTP 429 방지
DB_CHUNK_SIZE = 1000          # DB 커밋 청크 크기

# SMA/BBands warm-up 기간 (경계 NaN 방지를 위해 앞부분 여유 데이터)
SMA_PERIOD    = 20
RSI_PERIOD    = 14
BB_PERIOD     = 20


# ─────────────────────────────────────────────────────────────────────────
# 1. 거래소 초기화
# ─────────────────────────────────────────────────────────────────────────
def _build_exchange() -> ccxt.Exchange:
    """
    바이낸스 ccxt 인스턴스 생성 (익명/공개 모드).
    - OHLCV 수집은 Public API → API Key 불필요
    - .env에 테스트넷 키가 세팅되어 있어도 사용하지 않음
      (테스트넷 키를 메인넷에 전달하면 -2008 AuthenticationError 발생)
    """
    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",  # 현물 OHLCV; 선물이 필요하면 "future"로 변경
            },
        }
    )
    # API 키를 명시적으로 제거 (혹시 환경변수로 자동 주입될 경우 대비)
    exchange.apiKey = None
    exchange.secret = None

    logger.info("🌐 바이낸스 Mainnet 연결 (익명 모드 — 공개 OHLCV 수집용)")
    return exchange


# ─────────────────────────────────────────────────────────────────────────
# 2. 페이지네이션 OHLCV 수집
# ─────────────────────────────────────────────────────────────────────────
def fetch_ohlcv_paginated(
    exchange: ccxt.Exchange,
    symbol: str = SYMBOL,
    timeframe: str = TIMEFRAME,
    days_back: int = DAYS_BACK,
) -> list[list]:
    """
    바이낸스에서 페이지네이션으로 과거 OHLCV 데이터를 전부 수집.

    알고리즘:
      - since = 현재 UTC - days_back일
      - while since < now:
            batch = fetch_ohlcv(since=since, limit=API_LIMIT)
            since = batch[-1][0] + 1ms  (다음 캔들부터 이어받기)
      - 중복 제거 후 반환

    Returns:
        list of [timestamp_ms, open, high, low, close, volume]
    """
    now_ms    = exchange.milliseconds()
    since_ms  = now_ms - (days_back * 24 * 60 * 60 * 1000)

    logger.info(
        f"📡 수집 시작 | {symbol} {timeframe} | "
        f"기간: {_ms_to_dt(since_ms)} ~ {_ms_to_dt(now_ms)} "
        f"(최근 {days_back}일)"
    )

    all_candles: list[list] = []
    page        = 0
    current_since = since_ms

    while current_since < now_ms:
        page += 1
        try:
            batch = exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=current_since,
                limit=API_LIMIT,
            )
        except ccxt.RateLimitExceeded:
            logger.warning("⚠️  Rate Limit 초과 — 5초 대기 후 재시도")
            time.sleep(5)
            continue
        except ccxt.NetworkError as e:
            logger.error(f"❌ 네트워크 오류 (page={page}): {e} — 3초 후 재시도")
            time.sleep(3)
            continue
        except Exception as e:
            logger.error(f"❌ 예상치 못한 오류 (page={page}): {e}")
            raise

        if not batch:
            logger.info(f"  ↳ page {page}: 빈 응답 → 수집 완료")
            break

        all_candles.extend(batch)
        last_ts       = batch[-1][0]
        current_since = last_ts + 1  # 마지막 캔들 다음 ms 부터 재개

        elapsed_ratio = min(
            (last_ts - since_ms) / (now_ms - since_ms) * 100, 100.0
        )
        logger.info(
            f"  ↳ page {page:3d} | 배치={len(batch):4d}개 | "
            f"누적={len(all_candles):6d}개 | "
            f"마지막={_ms_to_dt(last_ts)} | "
            f"진행률={elapsed_ratio:.1f}%"
        )

        # 마지막 페이지 감지 (캔들 수가 Limit 미만이면 끝)
        if len(batch) < API_LIMIT:
            logger.info(f"  ↳ 배치 수 < {API_LIMIT} → 마지막 페이지 감지, 수집 완료")
            break

        time.sleep(SLEEP_SEC)

    # ── 중복 캔들 제거 (타임스탬프 기준) ──────────────────────────────────
    seen: set[int] = set()
    unique_candles: list[list] = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique_candles.append(c)

    unique_candles.sort(key=lambda c: c[0])

    logger.info(
        f"✅ 수집 완료: 총 {len(unique_candles):,}개 캔들 "
        f"({len(all_candles) - len(unique_candles)}개 중복 제거)"
    )
    return unique_candles


# ─────────────────────────────────────────────────────────────────────────
# 3. 피처 엔지니어링 (전체 DataFrame 한 번에 계산)
# ─────────────────────────────────────────────────────────────────────────
def compute_features(candles: list[list]) -> pd.DataFrame:
    """
    수집된 OHLCV 캔들 전체를 DataFrame으로 변환하고
    기술 지표를 일괄 계산.

    ⚠️  반드시 전체 데이터를 한 번에 계산해야 경계선 NaN이 발생하지 않음.

    지표:
        - SMA(20)            → 컬럼: SMA_20
        - RSI(14)            → 컬럼: RSI_14
        - Bollinger Bands(20, 2) → 컬럼: BBU_20_2.0 / BBL_20_2.0
    """
    logger.info(f"🔬 피처 엔지니어링 시작 ({len(candles):,}개 캔들)...")

    df = pd.DataFrame(
        candles,
        columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
    ).astype(
        {
            "open":   float,
            "high":   float,
            "low":    float,
            "close":  float,
            "volume": float,
        }
    )

    # ── 지표 계산 (전체 DataFrame에 일괄 적용) ────────────────────────────
    logger.info("  ↳ 데이터 타입 강제 캐스팅 (float)...")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    logger.info("  ↳ SMA(20) 계산 중...")
    df.ta.sma(length=SMA_PERIOD, append=True)          # → SMA_20

    logger.info("  ↳ RSI(14) 계산 중...")
    df.ta.rsi(length=RSI_PERIOD, append=True)          # → RSI_14

    logger.info("  ↳ Bollinger Bands(20, 2) 계산 중...")
    df.ta.bbands(length=BB_PERIOD, std=2, append=True) # → BBU_20_2.0, BBM_20_2.0, BBL_20_2.0

    # 볼린저 밴드 컬럼 동적 매핑 (Robust 방어)
    bb_upper_col = next((c for c in df.columns if c.startswith("BBU_")), None)
    bb_lower_col = next((c for c in df.columns if c.startswith("BBL_")), None)

    if bb_upper_col:
        df.rename(columns={bb_upper_col: "BB_UPPER"}, inplace=True)
    if bb_lower_col:
        df.rename(columns={bb_lower_col: "BB_LOWER"}, inplace=True)

    # NaN 통계 로깅
    nan_counts = {
        "SMA_20":   int(df["SMA_20"].isna().sum()) if "SMA_20" in df.columns else 0,
        "RSI_14":   int(df["RSI_14"].isna().sum()) if "RSI_14" in df.columns else 0,
        "BB_UPPER": int(df["BB_UPPER"].isna().sum()) if "BB_UPPER" in df.columns else 0,
        "BB_LOWER": int(df["BB_LOWER"].isna().sum()) if "BB_LOWER" in df.columns else 0,
    }
    logger.info(
        f"✅ 피처 계산 완료 | shape={df.shape} | "
        f"NaN 개수 (warm-up 기간): {nan_counts}"
    )
    return df


# ─────────────────────────────────────────────────────────────────────────
# 4. DB 벌크 Upsert
# ─────────────────────────────────────────────────────────────────────────
def bulk_upsert_to_db(df: pd.DataFrame, symbol: str = SYMBOL) -> None:
    """
    DataFrame의 모든 행을 market_data 테이블에 벌크 Upsert.

    - DB_CHUNK_SIZE(1,000)개 단위로 분할하여 메모리 초과 방지
    - ON CONFLICT (timestamp, symbol) → 지표 컬럼 업데이트 (OHLCV는 보존)
    """
    settings = get_settings()

    engine = create_engine(
        settings.sync_database_url,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=5,
    )
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    total_rows   = len(df)
    total_chunks = math.ceil(total_rows / DB_CHUNK_SIZE)

    logger.info(
        f"💾 DB 저장 시작 | 총 {total_rows:,}행 → "
        f"{DB_CHUNK_SIZE}개 청크 × {total_chunks}회 커밋"
    )

    def _to_dec(val) -> Decimal | None:
        """float/NaN → Decimal 또는 None 변환"""
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return Decimal(str(val))

    upserted_total = 0
    skipped_total  = 0

    for chunk_idx in range(total_chunks):
        start = chunk_idx * DB_CHUNK_SIZE
        end   = min(start + DB_CHUNK_SIZE, total_rows)
        chunk = df.iloc[start:end]

        rows: list[dict] = []
        for _, row in chunk.iterrows():
            ts_ms     = int(row["timestamp_ms"])
            candle_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

            rows.append(
                {
                    "timestamp": candle_dt,
                    "symbol":    symbol,
                    "open":      Decimal(str(row["open"])),
                    "high":      Decimal(str(row["high"])),
                    "low":       Decimal(str(row["low"])),
                    "close":     Decimal(str(row["close"])),
                    "volume":    Decimal(str(row["volume"])),
                    "sma_20":    _to_dec(row.get("SMA_20")),
                    "rsi_14":    _to_dec(row.get("RSI_14")),
                    "bb_upper":  _to_dec(row.get("BB_UPPER")),
                    "bb_lower":  _to_dec(row.get("BB_LOWER")),
                }
            )

        # ── 벌크 Upsert 실행 ──────────────────────────────────────────────
        insert_stmt = pg_insert(MarketData).values(rows)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_market_data_ts_symbol",
            set_={
                # OHLCV는 원본 유지, 지표 컬럼만 갱신
                "sma_20":   insert_stmt.excluded.sma_20,
                "rsi_14":   insert_stmt.excluded.rsi_14,
                "bb_upper": insert_stmt.excluded.bb_upper,
                "bb_lower": insert_stmt.excluded.bb_lower,
            },
        )

        session = Session()
        try:
            result = session.execute(upsert_stmt)
            session.commit()

            # rowcount: PostgreSQL INSERT ... ON CONFLICT 에서
            #   신규 삽입 = 1, 업데이트 = 2 로 카운트되므로 단순 참고용
            chunk_rows = end - start
            upserted_total += chunk_rows

            progress_pct = (end / total_rows) * 100
            logger.info(
                f"  ↳ 청크 {chunk_idx + 1:3d}/{total_chunks} "
                f"[{start:6d}~{end:6d}] "
                f"커밋 완료 | 진행률={progress_pct:.1f}%"
            )
        except Exception as e:
            session.rollback()
            logger.error(
                f"❌ DB 저장 실패 (청크 {chunk_idx + 1}): {e}", exc_info=True
            )
            raise
        finally:
            session.close()

    logger.info(
        f"✅ DB 저장 완료 | 총 {upserted_total:,}행 처리 "
        f"({total_chunks}회 청크 커밋)"
    )
    engine.dispose()


# ─────────────────────────────────────────────────────────────────────────
# 5. 유틸리티
# ─────────────────────────────────────────────────────────────────────────
def _ms_to_dt(ms: int) -> str:
    """밀리초 타임스탬프 → 사람이 읽기 쉬운 UTC 시각 문자열"""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


# ─────────────────────────────────────────────────────────────────────────
# 6. 메인 엔트리포인트
# ─────────────────────────────────────────────────────────────────────────
def main() -> None:
    start_time = time.perf_counter()

    logger.info("=" * 65)
    logger.info("🚀  QuantFlow — ML 학습 데이터 백필 스크립트 시작")
    logger.info(f"    대상: {SYMBOL}  타임프레임: {TIMEFRAME}  기간: 최근 {DAYS_BACK}일")
    logger.info(f"    예상 캔들 수: ~{DAYS_BACK * 24 * 60:,}개")
    logger.info("=" * 65)

    # ── Step 1: 거래소 초기화 ──────────────────────────────────────────
    exchange = _build_exchange()

    # ── Step 2: 페이지네이션 OHLCV 수집 ──────────────────────────────
    logger.info("\n[Step 1/3] 바이낸스 OHLCV 페이지네이션 수집")
    candles = fetch_ohlcv_paginated(
        exchange,
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        days_back=DAYS_BACK,
    )

    if not candles:
        logger.error("❌ 수집된 데이터가 없습니다. 스크립트를 종료합니다.")
        sys.exit(1)

    # ── Step 3: 피처 엔지니어링 ──────────────────────────────────────
    logger.info("\n[Step 2/3] 피처 엔지니어링 (지표 일괄 계산)")
    df = compute_features(candles)

    # ── Step 4: DB 벌크 Upsert ────────────────────────────────────────
    logger.info("\n[Step 3/3] PostgreSQL 벌크 Upsert")
    bulk_upsert_to_db(df, symbol=SYMBOL)

    elapsed = time.perf_counter() - start_time
    logger.info("")
    logger.info("=" * 65)
    logger.info(f"🎉  백필 완료! 총 소요 시간: {elapsed:.1f}초 ({elapsed / 60:.1f}분)")
    logger.info(
        f"    저장된 캔들: {len(df):,}개 | 지표 계산 포함 컬럼: {list(df.columns)}"
    )
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
