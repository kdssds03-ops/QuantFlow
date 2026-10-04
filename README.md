# QuantFlow

바이낸스 USDT 선물에서 24시간 돌아가는 암호화폐 자동매매 봇입니다.
Celery 워커가 1분마다 캔들을 수집하고 신호를 확인해 주문을 내며, 상태와 결과는 텔레그램으로 받습니다.

개인 프로젝트이고 실제 자금으로 운용 중입니다. 수익을 보장하지 않습니다.

## 전략

현재 쓰는 전략은 4시간봉 EMA(30/60) 교차 추세추종 하나입니다. 빠른 선이 느린 선 위에 있으면 롱, 아래면 숏이고, 신호가 뒤집히면 포지션을 반대로 바꿉니다(스톱 앤 리버스).

처음에는 1분봉 볼린저+RSI 평균회귀와 LightGBM 분류 모델로 시작했습니다. 라이브 로직을 그대로 옮긴 백테스트를 만들어 돌려 보니 둘 다 수수료를 빼면 엣지가 없어서 교체했습니다. 코드는 남아 있고 `PREDICTOR_TYPE`으로 고를 수 있지만 쓰지 않습니다.

검증구간(OOS, 438일) 기준 수치입니다. 백테스트는 실전보다 낙관적이니 이보다 낮게 봐야 합니다.

| 구성 | 결과 |
|---|---|
| BTC 단일, `RISK_FACTOR=0.50` | 연 +13.5%, MDD -18.7% |
| 메이저 4종 분산 + 변동성 타게팅 | Sharpe 0.78 → 1.10 (BTC 단일 대비) |

파라미터를 바꿀 때는 학습/검증 구간을 나누고, 한 값에서만 좋은 결과는 과최적으로 보고 버립니다. 피라미딩과 단일 심볼 변동성 타게팅은 이 기준에서 탈락해 꺼 두었습니다.

## 구성

```
postgres          1분봉, 체결 이력
redis             Celery 브로커, 락, 상태 플래그
api               FastAPI. 기동 시 Alembic 마이그레이션 수행, /health 제공
worker            캔들 수집 + 신호 분석 + 주문
listener-worker   텔레그램 명령 전용 (매매 워커와 큐 분리)
beat              스케줄러
```

`worker`와 `beat`는 `api`의 헬스체크가 통과한 뒤에 뜹니다. 마이그레이션이 끝나기 전에 워커가 DB를 건드리는 일을 막기 위해서입니다.

주요 코드:

- `worker/tasks.py` — 수집, 매매 판단, 주문 집행, 텔레그램 명령, 일일 리포트
- `worker/predictor.py` — 신호 생성 (TREND / RULE / ML)
- `worker/portfolio.py` — 멀티심볼 사이징 (리스크 패리티 + 포트 변동성 타게팅)
- `core/` — 설정, DB, 거래소(ccxt) 연결, 텔레그램 알림
- `scripts/` — 백테스트, 검증, 백필, 사전점검

## 리스크 관리

- **일일 손실 서킷브레이커**: 당일 손실이 `MAX_DAILY_LOSS_PCT`에 닿으면 신규 진입을 멈추고 다음 거래일(KST)에 자동 재개합니다. 보유 포지션의 손절은 계속 동작합니다.
- **자본 방화벽**: 한 번의 진입이 가용 마진의 `MAX_CAPITAL_PER_SYMBOL_PCT`를 넘으면 주문을 거부합니다. `RISK_FACTOR`보다 작게 두면 모든 주문이 거부되니 주의하세요.
- **재난 손절**: 추세 전환 신호와 별개로 -12% / -15%에서 강제 청산합니다.
- **중복 주문 방지**: 심볼별 Redis 락으로 같은 심볼의 동시 실행을 막습니다.
- **주문 집행**: 네트워크 오류는 지수 백오프로 3회 재시도, 잔고 부족·잘못된 주문은 즉시 거부, 5초 안에 체결되지 않은 잔량은 취소하고 실제 체결량만 기록합니다.
- **포트폴리오 모드 총노출 상한**: `PORTFOLIO_MAX_GROSS`(기본 1.5배)를 넘지 않습니다.

## 텔레그램

| 명령 | 동작 |
|---|---|
| `/status` | 잔고, 포지션(평단가·경과 시간), 24시간 손익, 현재가·지표 요약 |
| `/pause` | 신규 진입 중단 (보유 포지션 보호는 유지) |
| `/resume` | 재개 |

주문 체결, 손절, 서킷브레이커 발동 시 알림이 오고, 매일 23:59(KST)에 일일 결산이 옵니다. 12시간마다 도는 헬스체크는 이상이 있을 때만 알립니다.

## 실행

```bash
cp .env.example .env
```

`.env`에 DB 비밀번호, 거래소 API 키, 텔레그램 토큰을 채운 뒤:

```bash
docker compose up -d --build
```

```bash
docker compose logs -f worker
```

TREND 전략은 4시간봉이 최소 62개(1분봉 약 10일) 있어야 신호를 내고, 백테스트와 같은 EMA 값을 얻으려면 약 43일치가 필요합니다. 처음 띄울 때는 백필부터 하세요.

```bash
docker compose run --rm --entrypoint python worker scripts/backfill_symbol_1m.py "BTC/USDT" 45
```

설정을 바꾼 뒤 반영하는 방법이 다릅니다.

- `.env`를 바꿨으면 `docker compose up -d <서비스>` (`restart`는 env 파일을 다시 읽지 않습니다)
- 코드만 바꿨으면 `docker compose restart <서비스>`

## 주요 설정

| 변수 | 현재값 | 설명 |
|---|---|---|
| `EXCHANGE_SANDBOX` | `true` | 테스트넷 여부. 실전 전 반드시 페이퍼로 검증 |
| `PREDICTOR_TYPE` | `TREND` | `TREND` / `RULE` / `ML` |
| `RISK_FACTOR` | `0.50` | 진입 시 가용 마진 비율 |
| `MAX_DAILY_LOSS_PCT` | `0.05` | 서킷브레이커 기준 |
| `MAX_CAPITAL_PER_SYMBOL_PCT` | `0.60` | 자본 방화벽 |
| `PORTFOLIO_MODE` | `false` | 멀티심볼 분산 모드 |

포트폴리오 모드를 켜려면 전 심볼을 백필하고 테스트넷에서 몇 주 돌려 본 뒤에 소액으로 시작하세요. 나머지 변수는 `.env.example`에 설명이 있습니다.

## 실전 투입 전

```bash
python scripts/preflight.py
```

포지션 모드(One-Way), 레버리지, DB 데이터 신선도, API 키, 텔레그램 연결을 한 번에 점검합니다. 주문은 내지 않습니다.

백테스트와 검증 스크립트:

- `scripts/backtest_live.py` — 라이브 로직을 그대로 재현한 백테스트 (`--portfolio`로 멀티심볼)
- `scripts/risk_factor_scenarios.py` — 사이징별 OOS 시나리오
- `scripts/signal_research.py`, `scripts/regime_validation.py` — 신호·국면별 검증
- `scripts/audit_trend_fidelity.py` — 라이브와 백테스트의 괴리 측정

서버 배포는 [docs/DEPLOY_ORACLE.md](docs/DEPLOY_ORACLE.md)를 참고하세요.

## 알려진 한계

- 사이징은 진입할 때 한 번만 계산합니다. 매 봉 리밸런싱하는 백테스트보다 실측이 약간 나쁠 수 있습니다.
- 신호는 DB의 1분봉으로 계산하므로 수집이 끊기면 신호도 멈춥니다. 데이터가 부족하면 진입하지 않습니다.
- 가격 방향을 맞히는 모델이 아닙니다. 수익은 추세가 길게 이어질 때 나오고, 횡보장에서는 잦은 반전으로 손실이 납니다.
