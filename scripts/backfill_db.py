"""
scripts/backfill_db.py — market_data 테이블에 과거 1m OHLCV 백필

TREND 예측기는 4h EMA(30/60) 계산에 ~62개 4h봉(=약 10일 연속 1m)이 필요하다.
네트워크 끊김 등으로 DB 히스토리가 부족하면 TREND가 워밍업 동안 HOLD만 한다.
이 스크립트로 data/btc_1m_1year.csv의 과거 구간을 채워 즉시 작동하게 한다.

인디케이터 컬럼은 NULL로 두며(TREND는 close만 사용), 유니크 제약
(uq_market_data_ts_symbol) 충돌 시 OHLCV를 CSV 값으로 덮어쓴다(ON CONFLICT DO UPDATE).
— 과거 fetch_market_data_task가 '형성 중 캔들 스냅샷'(고가≈저가≈시가, 거래량≈0)을
  저장하던 결함으로 오염된 행을 이 스크립트 재실행으로 치유하기 위함이다.
  지표 컬럼은 건드리지 않는다 (TREND 신호는 close만 사용, 필요 시 warm-up이 재계산).

사용:
  python scripts/backfill_db.py [DB_URL] [start_YYYY-MM-DD] [end_YYYY-MM-DD]
  (기본: localhost DB, 2026-04-26 ~ 2026-05-26)
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

CSV = Path(__file__).resolve().parent.parent / "data" / "btc_1m_1year.csv"
DB_URL = sys.argv[1] if len(sys.argv) > 1 else "postgresql://quantflow:quantflow@localhost:5432/quantflow"
START = sys.argv[2] if len(sys.argv) > 2 else "2026-04-26"
END = sys.argv[3] if len(sys.argv) > 3 else "2026-05-26"
SYMBOL = "BTC/USDT"


def main():
    print(f"[백필] CSV={CSV.name}  구간 {START} ~ {END}  → {SYMBOL}")
    df = pd.read_csv(CSV, usecols=["timestamp", "open", "high", "low", "close", "volume"])
    dt = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    s = pd.Timestamp(START, tz="UTC")
    e = pd.Timestamp(END, tz="UTC")
    mask = (dt >= s) & (dt < e)
    df = df[mask].copy()
    df["dt"] = dt[mask]
    print(f"  대상 {len(df):,}행 ({df['dt'].min()} ~ {df['dt'].max()})")
    if df.empty:
        print("  대상 없음 — 종료")
        return

    rows = [
        (
            r.dt.to_pydatetime().astimezone(timezone.utc),
            SYMBOL, float(r.open), float(r.high), float(r.low), float(r.close), float(r.volume),
        )
        for r in df.itertuples(index=False)
    ]

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO market_data (timestamp, symbol, open, high, low, close, volume)
                   VALUES %s
                   ON CONFLICT ON CONSTRAINT uq_market_data_ts_symbol DO UPDATE SET
                       open   = EXCLUDED.open,
                       high   = EXCLUDED.high,
                       low    = EXCLUDED.low,
                       close  = EXCLUDED.close,
                       volume = EXCLUDED.volume""",
                rows, page_size=5000,
            )
            inserted = cur.rowcount
        conn.commit()
        print(f"  ✅ upsert 완료: {inserted:,}행 (기존 행은 OHLCV 갱신)")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM market_data WHERE symbol=%s", (SYMBOL,))
            total = cur.fetchone()[0]
        print(f"  현재 {SYMBOL} 총 캔들: {total:,}개 ({total/1440:.1f}일분)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
