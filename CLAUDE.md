# QuantFlow — Claude 작업 메모리

24시간 암호화폐 자동매매 봇 (Celery worker + FastAPI + Postgres + Redis, Docker Compose).
현재 전략: **4h EMA(30/60) 추세추종(`PREDICTOR_TYPE=TREND`) + `RISK_FACTOR=0.50`**(검증범위 상단 — 2026-06-12 사용자가 "최대 상향" 위임, OOS 438일 기준 연 +13.5%/MDD -18.7%).
변동성 타게팅(`VOL_TARGET_ENABLED`)은 라이브식(진입 1회 샘플링)이 OOS 열위(Sharpe 0.70→0.51)로 확인되어 **OFF**.
이전 1m 평균회귀(RULE/ML)는 백테스트상 엣지 0(수수료로 손실)임이 입증되어 교체됨.

---

## ⚠️ 반드시 지킬 규칙 (과거에 실제로 데인 것들 — 반복 금지)

### Docker — 변경 반영 방법을 혼동하지 말 것
- **`.env`(환경변수) 변경 → `docker compose up -d <svc>`** (컨테이너 재생성). `restart`는 env_file을 다시 안 읽는다. ❌ 과거에 `restart`로 `PREDICTOR_TYPE` 반영하려다 실패.
- **코드(.py) 변경 → `docker compose restart <svc>`** 면 충분. (단 아래 pyc 설정이 있어야 함)
- 코드/`.env` 둘 다 바꿨으면 `up -d`.

### Python `.pyc` staleness (Windows 바인드마운트)
- `__pycache__` 익명 볼륨 + Windows mtime 이슈로 **옛 `.pyc`가 코드 변경을 가린 적 있음**. 진단에 한참 걸림.
- 영구 해결됨: worker/beat/listener-worker에 **`PYTHONDONTWRITEBYTECODE=1`** 설정 (docker-compose.yml). 이제 `.pyc` 안 만들어 항상 소스 로드.
- **증상 재발 시**(코드 바꿨는데 옛 동작): `docker exec <svc> sh -c 'rm -f /opt/quantflow/**/__pycache__/*.pyc'` 후 restart. 또는 컨테이너 내 파일에 변경이 실제 있는지 `grep`으로 확인.

### pandas 타임스탬프 → epoch ms 변환 (이 세션에서 2번 틀림!)
- ❌ `series.astype('int64') // 10**6` — pandas 2.x는 datetime64 해상도가 ms/us라 이 나눗셈이 **틀린 값**을 준다 (11일치가 1봉으로 뭉개졌음).
- ✅ `series.values.astype('datetime64[ms]').astype('int64')` 또는 행별 `dt.timestamp() * 1000`.
- 리샘플 결과 봉 수가 비정상(예: 수천 행인데 1봉)이면 **즉시 타임스탬프 변환부터 의심**.

### 데이터/백테스트 — "너무 좋은 결과 = 버그/과최적" 먼저 의심
- **1m 수집은 완성봉만 저장**(`ts+60s <= now` 필터) + conflict 시 OHLCV도 갱신할 것. 과거 `fetch_market_data_task`가 형성 중 캔들 스냅샷(고가≈저가≈시가, 거래량≈0)을 INSERT하고 conflict 시 지표만 갱신해 **DB OHLCV가 영구 오염**됐었다(2026-06-12 수정). 오염 과거 구간은 `backfill_db.py`(현재 DO UPDATE) 재실행으로 치유.
- 수집 함수의 페이지네이션 버그로 **5.5개월치를 연율화해 AVAX +1016% 같은 거짓 수치**를 낼 뻔함(`len(b)<limit: break`가 첫 배치에서 조기 종료).
- **백테스트 수치를 믿기 전 반드시**: ① 수집 데이터의 **날짜 범위·행 수**를 출력해 확인, ② 결과가 화려하면 **내부 정합성 교차검증**(예: 같은 자산을 두 방법으로).
- 전략/파라미터 개선은 **학습/검증(IS/OOS) 분리 + 파라미터 고원**으로 검증. 한 값만 좋으면(예: ADX≥20만) 과최적이다. 단순함을 우선.

### Git Bash (Windows) 함정
- **커밋 메시지는 항상 파일로**: `git commit -F <file>`. `git commit -m "$(printf ...)"`에서 `%`·한글이 깨져 메시지가 잘렸다.
- `docker exec <svc> ... /opt/절대경로`는 MSYS가 `C:/Program Files/Git/opt/...`로 변환한다. **`MSYS_NO_PATHCONV=1` 프리픽스** 또는 `sh -c '...'`로 감쌀 것.

### 편집 후 검증
- Python 편집 후 `python -m py_compile <files>`로 즉시 문법/미정의 상수 확인(이 세션에서 `BARS_PER_YEAR_4H` 미정의를 컴파일로 잡음).

---

## 핵심 운영 정보

- **인프라(컨테이너 내부 통신)**: `POSTGRES_HOST=postgres`, `REDIS_URL=redis://redis:6379/0`. 호스트에서 직접 붙을 땐 `localhost`로 오버라이드.
- **스택**: postgres, redis, api(+alembic 마이그레이션 담당), worker(매매), listener-worker(텔레그램), beat(스케줄).
- **전략 토글(.env)**: `PREDICTOR_TYPE=TREND`, `RISK_FACTOR`(사이징, 현행 0.50), `VOL_TARGET_ENABLED=false`(현행), `MAX_DAILY_LOSS_PCT`(서킷브레이커, 현행 0.05), `MAX_CAPITAL_PER_SYMBOL_PCT`(자본 방화벽, 현행 0.60 — **반드시 RISK_FACTOR×vol_scale_max보다 클 것**, 작으면 전 주문 거부=무거래).
- **TREND 워밍업**: 신호 발화 최소 4h봉 ≥62개(≈10일 연속 1m), **백테스트와 EMA 일치엔 260개(≈43일) 필요**(predictor가 그만큼 조회 — 짧으면 EMA 시드 편향으로 4h봉의 ~3%에서 신호 뒤집힘). `scripts/backfill_db.py`로 과거 1m 백필 가능.
- **TREND 모드 자동 비활성 가드**: 타임아웃·하드TP·타이트SL·트레일링·**불타기(피라미딩)**. 피라미딩은 IS/OOS 분리 검증에서 과최적 기각(검증구간 전 지표 열위) — 재검토 시 `python scripts/audit_trend_fidelity.py --split-only` 재실행.
- **실전 전 점검**: `python scripts/preflight.py` (One-Way 모드·레버리지·DB신선도·API키 등).
- **검증 도구**: `scripts/backtest_live.py`(라이브 로직 충실 재현), `signal_research.py`/`regime_validation.py`/`portfolio_backtest.py`(엣지·국면·분산 검증), `fetch_ohlcv.py`(데이터 수집).
- **WS 한계(기존)**: Celery prefork 멀티프로세스라 WS 인메모리 큐를 태스크가 공유 못 해 REST 폴백. 4h 전략엔 무해(신호는 DB 기반).

## 정직성 원칙 (이 봇은 실자금이 걸림)
- 예측 모델로 가격을 못 맞힌다(≈동전던지기). 수익은 비대칭 손익+리스크 관리에서 나온다. **"수익 보장" 설계는 불가능** — 과장 금지.
- 백테스트는 실전보다 낙관적. 기대치는 검증구간(OOS) 기준으로 깎아서 제시.
