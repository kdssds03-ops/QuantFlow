import sys
import os
import time
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

# 프로젝트 루트 경로를 sys.path에 추가하여 내부 모듈 임포트 가능하게 설정
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from sqlalchemy.dialects.postgresql import insert as pg_insert
from worker.tasks import _get_sync_session
from worker.indicators import compute_all_features
from core.exchange import get_exchange
from app.models.models import MarketData
import pandas as pd
import pandas_ta as ta
import numpy as np

# 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def backfill_data(symbol="BTC/USDT", timeframe="1m", years=1):
    """
    바이낸스 API를 호출하여 과거 1년치 1분 봉 데이터를 페이징 방식으로 긁어오고,
    PostgreSQL DB에 중복 없이 Bulk Upsert(Do Nothing) 하는 백필 엔진입니다.
    """
    exchange = get_exchange()
    
    # 1. 시간 범위 설정: 현재 시점을 기준으로 과거 n년(기본 1년) 전의 타임스탬프 계산
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - relativedelta(years=years)
    
    # 바이낸스 API 규격에 맞게 밀리초(ms) 단위 Timestamp로 변환
    since_ms = int(start_time.timestamp() * 1000)
    end_ms = int(now_utc.timestamp() * 1000)
    
    logger.info("🚀 [QuantFlow Backfill Engine] 대용량 시계열 데이터 수집 시작")
    logger.info(f"• 타겟 심볼: {symbol} ({timeframe})")
    logger.info(f"• 조회 기간: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC ~ {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    total_collected = 0
    
    # 세션 제너레이터에서 세션 객체 추출
    session = next(_get_sync_session())
    
    try:
        # 2. 페이징(Pagination) 및 과거 탐색 루프
        while since_ms < end_ms:
            try:
                # 1회 호출 시 최대 1000개의 캔들을 요청
                ohlcvs = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=1000)
                
                if not ohlcvs:
                    logger.warning("⚠️ API 반환 데이터가 비어 있습니다. 탐색을 종료합니다.")
                    break
                
                # DataFrame 변환 및 피처 엔지니어링
                df = pd.DataFrame(
                    ohlcvs,
                    columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
                ).astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
                
                df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)
                df = compute_all_features(df)
                
                # 신규 지표 ATR & MACD 추가 (실시간 파이프라인과 동일)
                df.ta.atr(length=14, append=True)
                df.ta.macd(fast=12, slow=26, signal=9, append=True)

                atr_col = next((c for c in df.columns if 'atr' in c.lower()), None)
                macd_line_col = next((c for c in df.columns if c.lower().startswith('macd_')), None)
                macd_signal_col = next((c for c in df.columns if c.lower().startswith('macds_')), None)
                macd_hist_col = next((c for c in df.columns if c.lower().startswith('macdh_')), None)
                
                def _safe_val(val):
                    if pd.isna(val): return None
                    return val

                mappings = []
                for _, row in df.iterrows():
                    ts_ms = row["timestamp_ms"]
                    # UTC 시간대로 정확히 맵핑
                    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                    if int(dt.timestamp() * 1000) > end_ms:
                        continue

                    mappings.append({
                        "timestamp": dt,
                        "symbol": symbol,
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "sma_20": _safe_val(row.get("sma_20")),
                        "rsi_14": _safe_val(row.get("rsi_14")),
                        "bb_upper": _safe_val(row.get("bb_upper")),
                        "bb_lower": _safe_val(row.get("bb_lower")),
                        "atr_14": _safe_val(row.get(atr_col)) if atr_col else None,
                        "macd_line": _safe_val(row.get(macd_line_col)) if macd_line_col else None,
                        "macd_signal": _safe_val(row.get(macd_signal_col)) if macd_signal_col else None,
                        "macd_hist": _safe_val(row.get(macd_hist_col)) if macd_hist_col else None,
                    })
                
                # 다음 루프를 위한 since_ms 업데이트 (가장 마지막으로 수집된 캔들의 시간 + 1ms)
                last_ts = df["timestamp_ms"].iloc[-1]
                if since_ms == last_ts + 1:
                    logger.warning("⚠️ 타임스탬프가 진전되지 않습니다. 무한 루프 방지를 위해 종료합니다.")
                    break
                since_ms = last_ts + 1
                
                if not mappings:
                    break
                
                # 3. 중복 방지 및 Bulk Upsert (새로 추가된 지표 덮어쓰기)
                # 파라미터 개수 초과로 인한 SQLAlchemy 렉을 방지하기 위해 200개 단위로 분할 적재
                INSERT_CHUNK_SIZE = 200
                for i in range(0, len(mappings), INSERT_CHUNK_SIZE):
                    chunk = mappings[i:i + INSERT_CHUNK_SIZE]
                    stmt = pg_insert(MarketData).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["timestamp", "symbol"],
                        set_={
                            "sma_20": stmt.excluded.sma_20,
                            "rsi_14": stmt.excluded.rsi_14,
                            "bb_upper": stmt.excluded.bb_upper,
                            "bb_lower": stmt.excluded.bb_lower,
                            "atr_14": stmt.excluded.atr_14,
                            "macd_line": stmt.excluded.macd_line,
                            "macd_signal": stmt.excluded.macd_signal,
                            "macd_hist": stmt.excluded.macd_hist,
                        }
                    )
                    session.execute(stmt)
                
                session.commit()
                
                # 4. 진행률 로깅
                total_collected += len(mappings)
                last_dt_str = mappings[-1]["timestamp"].strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"⏳ [진행 중] 현재 시점: {last_dt_str} UTC | 현재까지 총 {total_collected:,}개 캔들 수집 완료")
                
                # 거래소 Rate Limit(429 Too Many Requests) 차단 방어용 안전 마진
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ 데이터 수집 중 예외 발생: {e}")
                session.rollback()
                # 일시적인 네트워크 장애일 수 있으므로 5초 대기 후 재시도
                logger.info("🔄 5초 대기 후 재시도합니다...")
                time.sleep(5)

    finally:
        session.close()
        logger.info(f"🎉 1년 치 백필 작업 종료! DB에 완벽히 각인된 총 수집 캔들 수: {total_collected:,}개")

if __name__ == "__main__":
    backfill_data()
