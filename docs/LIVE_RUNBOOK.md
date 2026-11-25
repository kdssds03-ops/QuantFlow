# 라이브 전환 절차서 (페이퍼 검증 → 소액 실전)

이 문서는 단일 BTC 모드(`PREDICTOR_TYPE=TREND`, `PORTFOLIO_MODE=false`)를 기준으로 한다.
서버 배포 방법은 [DEPLOY_ORACLE.md](DEPLOY_ORACLE.md)를 따른다.

**기대치:** 검증 구간(OOS) 기준으로 연 +13.5%, 최대낙폭(MDD) -18.7%이다. 실전은 이보다 나쁘다고 가정한다. 수익은 보장되지 않는다.

---

## 0. 사전 조건 (코드)

- 2026-11-25 감사에서 주문 경로의 결함을 고쳤다. 아래 테스트가 전부 통과해야 한다.
  ```bash
  .venv/bin/python -m pytest -q scripts/test_order_safety.py
  .venv/bin/python scripts/test_portfolio.py
  ```
- 코드나 `.env`를 바꿨다면 `docker compose up -d`로 반영한다. `restart`는 `.env`를 다시 읽지 않는다.

## 1. 페이퍼(데모) 가동 — 최소 7일, 권장 14일

1. `.env`를 다음과 같이 설정한다.
   - `EXCHANGE_SANDBOX=true`
   - 데모 계정 API 키(demo-fapi.binance.com)
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
   - `HEALTHCHECK_ALERT_ONLY=false`: 처음에는 정상일 때도 생존 신호를 받도록 한다.
2. 1m 데이터를 백필한다. TREND 신호가 백테스트와 일치하려면 43일 이상 필요하다.
   ```bash
   docker compose run --rm --no-deps --entrypoint python worker scripts/backfill_symbol_1m.py BTC/USDT 45
   ```
3. `docker compose up -d`를 실행한 뒤 `python scripts/preflight.py`가 FAIL 0건인지 확인한다.

### 페이퍼 통과 기준 (전부 충족해야 다음 단계)

| 항목 | 확인 방법 | 기준 |
|---|---|---|
| 신호 = 백테스트 | 기간 중 4h 신호 전환 시점을 `scripts/backtest_live.py` 결과와 비교 | 전환 시점 100% 일치 |
| 주문 정합성 | 거래소 데모 체결 내역과 `trade_history` 비교 | 누락·중복 0건 |
| 포지션 정합성 | 매일 거래소 포지션과 텔레그램 `/status` 비교 | 불일치 0건 |
| 경보 동작 | 아래 장애 훈련 3종 | 모두 텔레그램 수신 |
| 무중단 | 헬스체크 알림·로그 | 수집 정지(5분 초과 신선도 경보) 0건 |

### 장애 훈련 (페이퍼 중 1회씩 실시)

1. **Redis 중지:** `docker compose stop redis`를 실행하면 "Redis 상태 조회 실패 — 신규 진입 동결" 경보가 와야 한다. 그다음 `start`로 복구한다.
2. **수집 정지:** `docker compose stop beat` 후 6분 이상 기다리면 "시세 데이터 정지 — 신규 진입 동결" 경보가 와야 한다. 그다음 `start`로 복구한다.
3. **/pause → /resume:** 텔레그램 명령으로 신규 진입이 동결되고 해제되는지 확인한다.

## 2. 소액 실전 — 첫 4주

1. 거래소 계정 설정을 확인한다.
   - **One-Way 모드**
   - 레버리지 1~3x
   - API 키 출금 권한 OFF, IP 화이트리스트
2. 선물 지갑에는 **잃어도 되는 소액만** 넣는다. BTC 최소 주문 0.001(약 $100)을 감안해 $300~500 수준이 적당하다.
   - 가용 마진 × 0.5가 $100보다 작으면 0.001 BTC로 올려 진입한다.
   - 이때 자본 방화벽(`MAX_CAPITAL_PER_SYMBOL_PCT`) 한도를 넘으면 주문이 거부된다.
3. `.env`의 `EXCHANGE_SANDBOX=false`로 바꾸고 실전 키를 입력한 뒤 `docker compose up -d`로 반영한다.
4. `python scripts/preflight.py`를 실행한다. 실전 모드에서는 텔레그램이 설정되지 않으면 FAIL이다.
5. 첫 신호가 체결되면 거래소 앱에서 포지션과 수량을 직접 확인한다.

### 즉시 중단 조건

다음 중 하나라도 해당하면 텔레그램 `/pause`를 보낸다. 포지션이 남아 있으면 거래소에서 직접 청산한다.

- "주문 실패/상태 불명" 또는 "청산 미완료" 경보가 1회라도 발생
- 거래소 포지션과 봇 상태가 불일치
- 4주 낙폭이 -10%를 초과 (OOS MDD의 절반 이상이 이렇게 빠르게 오면 재검토)

## 3. 알려진 한계 (아직 미수정)

- **거래소 측 손절 주문이 없다.** 워커가 죽으면 포지션에 보호 장치가 없다.
  - 재난 손절(-12%)은 봇이 매분 확인하는 소프트 손절이다.
  - 그래서 헬스체크 생존 신호(`HEALTHCHECK_ALERT_ONLY=false`)가 중요하다.
- 서킷브레이커 문제:
  - 장중 재기동이나 입출금 후에는 기준 자본이 어긋날 수 있다.
  - 한도에 도달한 뒤 `/resume`을 보내도 같은 날 다시 PAUSED가 된다.
- 포트폴리오 모드 문제:
  - 사이징 분모가 가용 마진이라, 먼저 진입한 심볼이 있으면 나중 심볼이 작게 잡힌다.
  - 알트코인의 최소 주문금액(min notional) 검사가 없다.
  - **포트폴리오 모드는 단일 BTC 실전이 안정된 뒤 별도로 검증한다.**
