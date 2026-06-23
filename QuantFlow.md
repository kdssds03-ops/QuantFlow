# QuantFlow 진행 상태 (핸드오프) — 2026-06-23

> 새 대화에서 이 파일 + `CLAUDE.md`만 읽으면 그대로 이어서 작업 가능하도록 작성.

## 한 줄 요약
검증된 4h EMA(30/60) 추세 신호를 **메이저4(BTC/ETH/SOL/BNB)에 리스크패리티+변동성타게팅으로 분산**하는 `PORTFOLIO_MODE`를 구현·검증하고, **페이퍼(sandbox)로 정상 가동까지 완료**했다. 실자금 전환 전 남은 건 코드가 아니라 **수주간 페이퍼 관찰(시간)** 뿐.

---

## 1. 현재 상태 스냅샷
- **가동 중(페이퍼)**: `C:\dev\QuantFlow`, docker compose 프로젝트명 `quantflow`, `EXCHANGE_SANDBOX=true`(데모 = 실자금 아님).
- **전략**: PORTFOLIO_MODE, 심볼 BTC/ETH/SOL/BNB, `PORTFOLIO_LEVERAGE_K=1.0`, 총노출 하드캡 `PORTFOLIO_MAX_GROSS=1.5x`.
- **최근 관측 로그**: 포트 변동성 ~49% → vol타게팅이 노출을 `gross≈0.31`로 보수적 축소, 가중치 {BTC .30, BNB .30, ETH .21, SOL .19}, 포지션 2롱(ETH/SOL)·2숏(BTC/BNB), **에러 0**, 전 `analyze_and_trade` succeeded.
- **데이터**: 4심볼 1m 캔들 ≥45일 백필 완료(BTC 58일), 현재 시각까지 최신. DB는 볼륨 `quantflow_pg_data`에 보존(스택 down해도 유지).
- **자동 점검**: 매일 **09:09** 페이퍼 상태 요약 스케줄 작업 `quantflow-paper-daily-check` 등록됨(앱 켜져 있을 때 실행, 꺼져 있었으면 다음 실행 시).

## 2. 왜 이 전략인가 (백테스트 근거)
- 단일 BTC(현행): CAGR **+23%** / MDD **−50%** / Sharpe 0.68.
- 메이저4 분산+변동성타게팅: **OOS Sharpe 0.78→1.10**, MDD 대폭↓(낙폭이 진짜 이득).
- 절대수익은 레버 `k`로 통제(권장 1.0; 메이저4는 k>3에서 수익↓·낙폭↑·청산위험 — 8코인과 달리 레벨이 낮은 대신 DOGE/AVAX 의존 없음).
- 신호 정교화(돈치안 앙상블, 저타임프레임, 알트 체리피킹)는 OOS에서 거부됨 → **단순 유지**.
- 재현: `python scripts/improve_research.py`, `python scripts/backtest_live.py --portfolio`.

## 3. 코드 변경 (파일별)
- **NEW `worker/portfolio.py`** — 사이징 순수함수(리스크패리티 = 변동성역수가중 + 포트 변동성타게팅 + 레버 k + gross 하드캡, 심볼당 자본방화벽 클램프). 데이터 부족·예외 시 0(진입 보류) 안전착지. 단위테스트 `scripts/test_portfolio.py`(5/5 통과).
- **`worker/tasks.py`** — ① `_EFFECTIVE_RISK`에 `portfolio_effective_risk()` 주입(토글 ON일 때만, lazy import) ② `_fetch_1m_closes()` 헬퍼 추가 ③ 기동 방화벽 점검 포트모드 인지화 ④ **동기 DB 엔진 `poolclass=NullPool`로 변경(중요 버그픽스, 아래 4-②)**.
- **`worker/celery_app.py`** — beat 스케줄을 포트모드면 4심볼 fan-out(fetch+analyze ×4), 아니면 BTC 단일.
- **`core/config.py` / `.env.example`** — `PORTFOLIO_MODE / PORTFOLIO_SYMBOLS / PORTFOLIO_LEVERAGE_K / PORTFOLIO_TARGET_VOL / PORTFOLIO_MAX_LEV / PORTFOLIO_MAX_GROSS` 추가(기본 OFF).
- **`docker-compose.yml`** — 4개 서비스의 `- .:/opt/quantflow` **바인드마운트 제거**(아래 4-①). 이제 이미지에 구운 코드로 실행.
- **NEW `scripts/backfill_symbol_1m.py`** — 임의 심볼 1m을 바이낸스→DB upsert(검증된 페이지네이션+완성봉 필터 재사용). `backfill_db.py`/`fetch_ohlcv.py`는 BTC·CSV 전용이라 추가.
- **`CLAUDE.md`** — 포트폴리오 모드 메모 추가.

## 4. 페이퍼 기동 중 발견·수정한 버그 3개 (재발 방지)
1. **바인드마운트 인코딩 손상(`UnicodeDecodeError 0xc6 @2955`)**: OneDrive 폴더를 Docker에 바인드마운트하면 celery가 4개 포크로 소스를 **동시 읽을 때** Windows 파일공유가 깨진 바이트를 물려줌(호스트·일회성 컨테이너는 정상). → **바인드마운트 제거 + 이미지 베이크**로 우회. (그래서 코드 변경 시 `build` 필요)
2. **fork DB 동시성(`psycopg2 PGRES_TUPLES_OK and no message`)**: 동기 SQLAlchemy 엔진이 풀(`pool_size=5`)로 부모에서 생성→4포크가 공유→동시 사용 시 커넥션 손상. 단일 BTC일 땐 안 터지다가 4심볼 동시 실행에서 드러남. → `poolclass=NullPool`(async 엔진이 이미 쓰던 방식)로 수정. **확인: 수정 후 24h 에러 0.**
3. **데이터 부재**: ETH/SOL/BNB 1m이 DB에 없었음(봇이 BTC만 수집했었고 그마저 14일 정체). → `backfill_symbol_1m.py`로 4심볼 45일 백필.

## 5. ⚠️ 배포 구조 & 변경 적용법 (가장 헷갈리는 부분)
- **코드 편집/깃**: `C:\Users\kdssd\OneDrive\바탕 화면\QuantFlow` (여기에 `.git` 있음).
- **실제 실행**: `C:\dev\QuantFlow` (바인드마운트 없음 = **이미지에 구운 코드**로 구동). 백필 데이터 볼륨은 프로젝트명 `quantflow`로 공유되어 두 위치 무관하게 동일.
- **코드 변경 반영 절차**:
  1. OneDrive 폴더에서 편집
  2. 변경 파일을 `C:\dev\QuantFlow`로 복사 (예: `copy "...\OneDrive\...\QuantFlow\worker\tasks.py" "C:\dev\QuantFlow\worker\tasks.py"`)
  3. `cd "C:\dev\QuantFlow"; docker compose -p quantflow build; docker compose -p quantflow up -d`
- ⚠️ **바인드마운트가 없으므로 라이브 리로드 안 됨** — 코드 바꾸면 반드시 `build`.
- 💡 권장: 장기적으론 한 위치로 통합 고려(예: `C:\dev\QuantFlow`에 git 이전). 단, Cowork 파일도구는 OneDrive 폴더만 접근 가능.

## 6. 운영 명령 (PowerShell, `cd "C:\dev\QuantFlow"` 후)
- 상태: `docker compose -p quantflow ps`
- 로그: `docker compose -p quantflow logs -f worker`
- 사이징/포지션 확인: `docker compose -p quantflow logs worker --since 10m | Select-String "PORTFOLIO\]|trigger_type|TrendFollow"`
- **롤백(단일 BTC로 복귀)**: `.env`에서 `PORTFOLIO_MODE=false` → `docker compose -p quantflow build; docker compose -p quantflow up -d`
- 정지(데이터 보존): `docker compose -p quantflow down`

## 7. .env 핵심값 (현재)
```
EXCHANGE_SANDBOX=true        # 데모/페이퍼. 실거래 시에만 false
PREDICTOR_TYPE=TREND
RISK_FACTOR=0.50             # 단일심볼 모드용(포트모드에선 미사용)
MAX_DAILY_LOSS_PCT=0.05      # 일일 손실 서킷브레이커
PORTFOLIO_MODE=true
PORTFOLIO_SYMBOLS=BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT
PORTFOLIO_LEVERAGE_K=1.0
PORTFOLIO_TARGET_VOL=0.15
PORTFOLIO_MAX_LEV=3.0
PORTFOLIO_MAX_GROSS=1.5      # 총노출 하드캡(청산 방지)
```

## 8. 남은 일 → 실자금 로드맵
1. **[지금] 페이퍼 수주 관찰** — 일일리포트(매일 23:59)·텔레그램·자동점검(09:09)으로 ① 에러 누적 0 유지 ② 낙폭이 감내 범위 ③ live가 backtest와 어긋나지 않는지 확인. 추세장·횡보장 둘 다 겪어보는 게 이상적.
2. **통과 시** `.env`에서 `EXCHANGE_SANDBOX=false` + **잃어도 되는 소액**으로 실거래. ← 이 스위치는 **사용자가 직접** 누름.
3. 실측이 페이퍼와 맞으면 점진 증액.

## 9. 정직성 (실자금 경고)
- 백테스트는 실전보다 낙관적: 알트 **슬리피지·펀딩비·청산·변동성드래그 미반영**. 메이저4 k=1 기대치 +9%/yr·MDD−18%이고 **실측은 깎아서** 봐야 함.
- 사이징은 진입 시 1회 샘플링이라 매봉 리밸런싱 백테스트보다 약간 열위 가능(분산 이득은 유효).
- 수익의 출처는 가격예측이 아니라 **분산+리스크관리**. "수익 보장" 설계 불가.
- 레버 `k`는 양날 — 키울수록 수익·낙폭 동시 증가. 메이저4는 k>2~3이 비효율(낙폭만↑).

## 10. 미해결 / 선택 개선거리
- 로그의 수량 단위가 모두 `BTC`로 표기됨(하드코딩 라벨, **표시상 버그 — 로직엔 무관**). 멀티심볼 로그 가독성 위해 심볼별 라벨로 정리 가능.
- 두 폴더(OneDrive 편집 / C:\dev 실행) **통합 여부** 결정.
- (재검토 시) 8코인 바스켓은 수익↑이나 슬리피지·DOGE/AVAX 의존이 커 메이저4가 견고. 필요하면 `improve_research.py`로 재확인.
- WS 인메모리 큐 미성숙으로 REST 폴백 사용 중(4h 전략엔 무해).

## 11. 검증 도구 모음
- `scripts/improve_research.py` — 분산/레버/파라미터고원/앙상블 IS·OOS 검증
- `scripts/backtest_live.py --portfolio` — 라이브 충실 포트 백테스트(+레버 프런티어)
- `scripts/test_portfolio.py` — 사이징 단위테스트
- `scripts/backfill_symbol_1m.py "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT" 45` — 1m 백필
- `scripts/preflight.py` — 실전 전 점검(레버리지·One-Way·DB신선도 등)
