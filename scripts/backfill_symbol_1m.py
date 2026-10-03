"""
scripts/backfill_symbol_1m.py — 임의 심볼 1m OHLCV를 바이낸스에서 받아 DB(market_data)에 upsert.

BTC 단일·포트폴리오 공용 백필 도구 (구 backfill_db.py/backfill_data.py 대체).
  - 페이지네이션: cur = batch[-1][0] + 60_000  (fetch_ohlcv.py — 조기종료 버그 없음)
  - upsert     : ON CONFLICT ON CONSTRAINT uq_market_data_ts_symbol DO UPDATE
                 (오염된 과거 OHLCV도 재실행으로 치유)

⚠️ CLAUDE.md 데이터 함정 준수:
  - 완성봉만 저장: ts + 60_000 <= now_ms 필터 (형성 중 캔들 오염 방지)
  - 타임스탬프: ccxt epoch ms(int) → datetime.fromtimestamp(ms/1000, tz=utc) 행별 변환
    (pandas astype('int64')//10**6 변환 금지)
  - 지표 컬럼은 NULL로 둠 (TREND/포트는 close만 사용; 워밍업이 재계산)

사용(컨테이너 내):
  docker compose run --rm --no-deps --entrypoint python worker \
      scripts/backfill_symbol_1m.py "ETH/USDT,SOL/USDT,BNB/USDT" 45
인자: [symbols(쉼표구분, 기본 ENV PORTFOLIO_SYMBOLS 또는 메이저4)] [days(기본 45)] [DB_URL]
"""
import os
import sys
import time
from datetime import datetime, timezone

import ccxt
import psycopg2
from psycopg2.extras import execute_values


def _db_url() -> str:
    if len(sys.argv) > 3 and sys.argv[3]:
        return sys.argv[3]
    user = os.environ.get("POSTGRES_USER", "quantflow")
    pw = os.environ.get("POSTGRES_PASSWORD", "quantflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "quantflow")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def fetch_1m(ex, symbol: str, days: int) -> list:
    """완성된 1m 캔들만 [(ts_ms, o,h,l,c,v), ...] 반환 (검증된 페이지네이션)."""
    now_ms = ex.milliseconds()
    since = now_ms - days * 86_400_000
    rows, cur, req = [], since, 0
    print(f"[fetch] {symbol} 1m {days}일치 수집 시작...", flush=True)
    while cur < now_ms:
        try:
            batch = ex.fetch_ohlcv(symbol, "1m", since=cur, limit=1500)
        except Exception as e:
            print(f"  재시도 ({type(e).__name__}): {str(e)[:80]}", flush=True)
            time.sleep(2.0)
            continue
        if not batch:
            break
        rows.extend(batch)
        cur = batch[-1][0] + 60_000          # 검증된 진행 방식 (조기종료 버그 없음)
        req += 1
        if req % 25 == 0:
            print(f"  ...{len(rows):,}봉 ({req}req)", flush=True)
        time.sleep(0.05)
    # 완성봉만 + 중복 제거 (ts 기준)
    seen = set()
    out = []
    for b in rows:
        ts = int(b[0])
        if ts + 60_000 > now_ms:             # 형성 중(미완성) 캔들 제외
            continue
        if ts in seen:
            continue
        seen.add(ts)
        out.append((ts, float(b[1]), float(b[2]), float(b[3]), float(b[4]), float(b[5])))
    out.sort(key=lambda x: x[0])
    return out


def upsert(conn, symbol: str, candles: list) -> int:
    rows = [
        (datetime.fromtimestamp(ts / 1000, tz=timezone.utc), symbol, o, h, l, c, v)
        for (ts, o, h, l, c, v) in candles
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO market_data (timestamp, symbol, open, high, low, close, volume)
               VALUES %s
               ON CONFLICT ON CONSTRAINT uq_market_data_ts_symbol DO UPDATE SET
                   open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                   close=EXCLUDED.close, volume=EXCLUDED.volume""",
            rows, page_size=5000,
        )
    conn.commit()
    return len(rows)


def main():
    default_syms = os.environ.get("PORTFOLIO_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT")
    syms_arg = sys.argv[1] if len(sys.argv) > 1 else default_syms
    symbols = [s.strip() for s in syms_arg.split(",") if s.strip()]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 45
    url = _db_url()
    print(f"[백필] 심볼={symbols} days={days} DB={url.split('@')[-1]}", flush=True)

    ex = ccxt.binanceusdm({"enableRateLimit": True})
    conn = psycopg2.connect(url)
    try:
        for s in symbols:
            candles = fetch_1m(ex, s, days)
            if not candles:
                print(f"  ⚠️ {s}: 수집 0봉 — 스킵", flush=True)
                continue
            n = upsert(conn, s, candles)
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM market_data WHERE symbol=%s", (s,))
                tot, mn, mx = cur.fetchone()
            print(f"  ✅ {s}: upsert {n:,}봉 → DB 총 {tot:,}봉 ({tot/1440:.1f}일), {mn} ~ {mx}", flush=True)
    finally:
        conn.close()
    print("[done] 백필 완료", flush=True)


if __name__ == "__main__":
    main()
