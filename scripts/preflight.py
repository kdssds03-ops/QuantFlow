"""
scripts/preflight.py — 실전 투입 전 사전점검 (읽기 전용, 주문 없음)

거래소/DB/Redis 연결, 포지션 모드, 레버리지, 데이터 신선도, 텔레그램,
서킷브레이커 설정 등 실전 가동에 필요한 항목을 한 번에 점검한다.
worker.tasks는 import 부작용(텔레그램/WS/워밍업)이 있어 의존하지 않는다.

사용: python scripts/preflight.py
"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

PASS, WARN, FAIL = "✅ PASS", "⚠️  WARN", "❌ FAIL"
results = []


def check(name, status, detail=""):
    results.append((status, name, detail))
    print(f"{status}  {name}" + (f" — {detail}" if detail else ""))


def main():
    from core.config import get_settings
    s = get_settings()

    print("=" * 60)
    print(" QuantFlow 실전 사전점검 (preflight)")
    print("=" * 60)
    mode = "🔴 실전(Mainnet)" if not s.exchange_sandbox else "🟡 샌드박스(Demo)"
    import os
    print(f"  거래소 모드 : {mode}")
    print(f"  예측 엔진   : {os.getenv('PREDICTOR_TYPE', 'RULE')}")
    print(f"  진입 사이징 : {s.risk_factor*100:.0f}% (RISK_FACTOR)")
    print(f"  서킷브레이커: {('-'+str(s.max_daily_loss_pct*100)+'%') if s.max_daily_loss_pct>0 else '비활성(0)'}")
    print("-" * 60)

    # ── 설정 위생 ──
    if s.secret_key == "change-me":
        check("SECRET_KEY 변경", WARN, "기본값 'change-me' — 변경 권장")
    else:
        check("SECRET_KEY 변경", PASS)
    if not (0 < s.risk_factor <= 1):
        check("RISK_FACTOR 범위", FAIL, f"{s.risk_factor} (0~1 벗어남)")
    else:
        check("RISK_FACTOR 범위", PASS if s.risk_factor <= 0.5 else WARN,
              f"{s.risk_factor*100:.0f}%" + ("" if s.risk_factor <= 0.5 else " — 공격적"))
    if not s.exchange_sandbox and s.max_daily_loss_pct <= 0:
        check("일일 손실 서킷브레이커", WARN, "실전인데 비활성 — MAX_DAILY_LOSS_PCT 설정 강력 권장")
    else:
        check("일일 손실 서킷브레이커", PASS if s.max_daily_loss_pct > 0 else WARN,
              f"-{s.max_daily_loss_pct*100:.1f}%" if s.max_daily_loss_pct > 0 else "비활성")

    # ── Redis ──
    try:
        import redis
        r = redis.Redis.from_url(s.redis_url, decode_responses=True, socket_connect_timeout=3)
        r.ping()
        sys_status = r.get("quantflow:sys_status")
        check("Redis 연결", PASS, f"sys_status={sys_status or '없음(RUNNING)'}")
    except Exception as e:
        check("Redis 연결", FAIL, str(e)[:60])

    # ── DB 연결 + 데이터 신선도 ──
    try:
        from sqlalchemy import create_engine, desc, func
        from sqlalchemy.orm import sessionmaker
        from app.models.models import MarketData
        eng = create_engine(s.sync_database_url, pool_pre_ping=True)
        Sess = sessionmaker(bind=eng)
        sess = Sess()
        try:
            cnt = sess.query(func.count(MarketData.id)).filter(MarketData.symbol == "BTC/USDT").scalar()
            latest = sess.query(MarketData).filter(MarketData.symbol == "BTC/USDT").order_by(desc(MarketData.timestamp)).first()
        finally:
            sess.close()
        check("DB 연결", PASS, f"BTC/USDT {cnt:,}봉 보유")
        if latest is not None:
            ts = latest.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
            check("DB 데이터 신선도", PASS if age_min < 5 else WARN,
                  f"최신 캔들 {age_min:.0f}분 전" + ("" if age_min < 5 else " — fetch 태스크 점검"))
            # TREND 모드는 ~10일(14400분) 1m 히스토리 필요
            days = cnt / 1440
            if os.getenv("PREDICTOR_TYPE", "RULE").upper() == "TREND":
                check("TREND 워밍업 데이터", PASS if days >= 10 else WARN,
                      f"{days:.1f}일분 (필요 ~10일)")
        else:
            check("DB 데이터", FAIL, "BTC/USDT 캔들 없음 — fetch_market_data 먼저 가동")
    except Exception as e:
        check("DB 연결", FAIL, str(e)[:80])

    # ── 거래소 연결 + 키 검증 + 포지션모드 + 레버리지 ──
    try:
        from core.exchange import get_exchange
        ex = get_exchange()
        bal = ex.fetch_balance({"type": "future"})
        usdt = bal["total"].get("USDT", 0)
        check("거래소 API 키", PASS, f"선물 잔고 조회 OK (USDT {float(usdt):,.2f})")
        # 포지션 모드 (One-Way 필수)
        try:
            dual = ex.fapiPrivateGetPositionSideDual()
            is_hedge = str(dual.get("dualSidePosition", "false")).lower() == "true"
            check("One-Way 포지션 모드", FAIL if is_hedge else PASS,
                  "Hedge 모드 감지 — 봇 오작동! One-Way로 변경 필요" if is_hedge else "One-Way 확인")
        except Exception as e:
            check("포지션 모드 확인", WARN, f"조회 실패: {str(e)[:50]}")
        # 레버리지
        try:
            pos = ex.fetch_positions(["BTC/USDT"])
            lev = None
            for p in pos:
                lev = p.get("leverage") or p.get("info", {}).get("leverage")
                if lev:
                    break
            if lev:
                lev = float(lev)
                check("레버리지", PASS if lev <= 3 else WARN,
                      f"{lev:.0f}x" + ("" if lev <= 3 else " — 고배율! 파산위험, 1~3x 권장"))
            else:
                check("레버리지 확인", WARN, "값 미검출 — 거래소에서 직접 확인(1~3x 권장)")
        except Exception as e:
            check("레버리지 확인", WARN, f"조회 실패: {str(e)[:50]}")
    except Exception as e:
        check("거래소 API 키", FAIL, str(e)[:80])

    # ── 텔레그램 ──
    if s.telegram_bot_token and s.telegram_chat_id:
        check("텔레그램 설정", PASS, "토큰/챗ID 존재 (/pause·알림 가능)")
    else:
        check("텔레그램 설정", WARN, "미설정 — 알림/원격 /pause 불가")

    # ── 요약 ──
    print("-" * 60)
    n_fail = sum(1 for st, _, _ in results if st == FAIL)
    n_warn = sum(1 for st, _, _ in results if st == WARN)
    print(f" 결과: {len(results)}개 점검 | ❌ {n_fail} | ⚠️  {n_warn} | ✅ {len(results)-n_fail-n_warn}")
    if n_fail:
        print(" 🚫 FAIL 항목을 해결하기 전에는 실전 투입 금지.")
    elif n_warn:
        print(" ⚠️  WARN 항목을 검토 후 진행하세요.")
    else:
        print(" ✅ 모든 점검 통과.")
    print("=" * 60)
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
