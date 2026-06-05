"""바이낸스 USDM 선물 1분봉 대량 수집 → data/btc_1m_1year.csv (백테스터용)."""
import sys, time
from pathlib import Path
import pandas as pd
import ccxt

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 180
SYMBOL = "BTC/USDT"
OUT = Path(__file__).resolve().parent.parent / "data" / "btc_1m_1year.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

ex = ccxt.binanceusdm({"enableRateLimit": True})
end = ex.milliseconds()
since = end - DAYS * 24 * 60 * 60 * 1000
all_rows = []
cur = since
req = 0
print(f"[fetch] {SYMBOL} 1m, {DAYS}일치 수집 시작...", flush=True)
while cur < end:
    try:
        batch = ex.fetch_ohlcv(SYMBOL, "1m", since=cur, limit=1500)
    except Exception as e:
        print(f"  재시도 ({type(e).__name__}): {str(e)[:80]}", flush=True)
        time.sleep(2.0)
        continue
    if not batch:
        break
    all_rows.extend(batch)
    cur = batch[-1][0] + 60_000
    req += 1
    if req % 20 == 0:
        print(f"  ...{len(all_rows):,}봉 ({req}req)", flush=True)
    time.sleep(0.05)

df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
df.drop_duplicates(subset=["timestamp"], inplace=True)
df.sort_values("timestamp", inplace=True)
df.reset_index(drop=True, inplace=True)
df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
df.to_csv(OUT, index=False)
span = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]) / 86_400_000
print(f"[done] {len(df):,}봉 저장 → {OUT.name} "
      f"({df['datetime'].iloc[0]} ~ {df['datetime'].iloc[-1]}, {span:.1f}일)", flush=True)
