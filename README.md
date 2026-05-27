# 📈 QuantFlow: Hybrid ML Trading & Real-time Orchestration System

> **High-Performance Distributed Algorithmic Trading Infrastructure with LightGBM & Celery**

QuantFlow는 실시간 금융 시계열 데이터 파이프라인 수집, 머신러닝(LightGBM) 기반의 하이브리드 의사결정, 그리고 금융권 프로덕션 규격의 주문 예외 처리 가드가 결합된 **상용 등급(Production-ready) 자동매매 인프라 시스템**입니다.

---

## 🛠️ Tech Stacks & Architecture

- **Backend Framework**: FastAPI (Asynchronous Concurrency)
- **Distributed Task Queue**: Celery (Prefork Worker Pool) & Redis (Message Broker)
- **Database**: PostgreSQL (Timescale-ready Time Series Layout) & Alembic (Schema Data Shield)
- **Machine Learning**: LightGBM, Scikit-Learn, Pandas, Numpy (In-Memory Vectorized Pipelines)
- **Exchange Interface**: CCXT Premium Connector (Binance / Bybit Sandbox Environment)
- **Monitoring**: Telegram Watchtower API (Premium HTML Formatted Report)

---

## 💡 Key Architectural Safeguards (핵심 가드 아키텍처)

### 1. 🛡️ 파일 시스템 기반 부팅 멱등성 가드 (Idempotency Shield)

Celery `prefork` 실행 모델의 프로세스 분기 및 모듈 재평가로 인한 텔레그램 알림 도배 장해를 방지하기 위해, 로컬 파일 시스템 마킹(`.welcome_sent`) 매커니즘을 구축하여 인프라 부팅 시 **생애 최초 단 1회의 알림만 발송**되도록 통제합니다.

### ⛓️ 2. 단독 책임 체인 의존성 인프라 (Dependency Chain Lifecycle)

다중 컨테이너 구동 시 발생하는 마이그레이션 레이스 컨디션을 원천 차단합니다. `api` 서비스가 `Alembic` 증분 마이그레이션을 안전하게 완수하고 `GET /health` 생존 심장박동을 증명하기 전까지 `worker`와 `beat` 컨테이너를 대기실에 묶어두는 안정적 오케스트레이션을 보장합니다.

### 🔄 3. 순환 참조 진압 및 지연 임포트 (Lazy Import Pattern)

앱 초기화(Initialization) 시점에 발생할 수 있는 모듈 간의 순환 의존성 분기를 차단하기 위해 글로벌 임포트를 전면 제거하고, API 라우터 함수 내부에 **Lazy Import 가드**를 주입하여 프로세스 무결성을 확보했습니다.

### 🛑 4. 무적의 주문 집행 파이프라인 (Order Execution Pipeline)

실전 트레이딩에서 발생하는 돌발 변수를 통제하기 위해 4단계 예외 처리 레이어를 탑재했습니다:

- **Network Timeout**: `tenacity` 라이브러리를 활용한 3단계 지수 백오프(Exponential Backoff: 1s -> 2s -> 4s) 재시도 가드
- **Insufficient Funds / Invalid Order**: 즉시 `REJECTED` 처리 후 단락(Short-circuit) 및 🚨 긴급 텔레그램 경고 발송
- **Unfilled Order Lock**: 주문 전송 후 최대 5초간 1초 주기로 체결 상태 폴링 스캔, 미체결 잔량은 `cancel_order()`로 강제 취소 후 실제 체결량 기준 DB 칼정산

### 🧠 5. 인메모리 피처 파이프라인 & ML 상식 검증 가드

- **In-Memory Logic**: DB 스키마 추가에 따른 I/O 병목 및 데이터 유실 위험을 방지하기 위해 최근 200봉 데이터를 판다스로 빌드하여 메모리 단에서 12종 다중 지표(EMA, MACD, RSI, Stochastic, BB)를 벡터 연산합니다.
- **Heuristic Guard**: LightGBM 모델의 추론 신뢰도(Confidence Threshold)가 65% 미만이거나, 과열/과매도 구간에서 모순되는 시그널(RSI 과열 시 BUY 발생 등)을 출력할 경우 의사결정을 강제로 `HOLD`로 스위칭하며, 예외 발생 시 규칙 기반 모델(`RuleBasedPredictor`)로 안전하게 폴백(Fallback)됩니다.

### 🔤 6. 바이낸스-CCXT 포지션 심볼 정규화 가드 (Symbol Normalization Shield) [v9.3]

바이낸스 선물 API(`positionRisk`)가 반환하는 하이픈/슬래시 없는 원시 심볼(예: `BTCUSDT`)과 로컬 DB/CCXT 등에서 사용하는 표준 심볼 포맷(예: `BTC/USDT`) 간의 불일치를 방지하기 위해 **심볼 정규화 엔진(`_normalize_symbol`)**을 탑재했습니다.
- **KeyMismatch 원천 차단**: 실시간으로 최고 수익률을 추적하는 인메모리 Peak ROI 레지스터(`_peak_roi_register`) 및 트레일링 스탑(Trailing Stop) 작동 시 발생할 수 있는 KeyMismatch 누락을 완벽히 방지합니다.
- **포지션 가드 무결성**: 어떠한 심볼 포맷의 입력이 들어오더라도 동일한 키값으로 매핑하여 트레일링 가드가 오작동하거나 누락되지 않도록 프로세스 무결성을 보장합니다.

---

## 📱 Real-time Telegram Monitoring Control & Daily Reporting [v9.3]

시스템은 실시간 매매 타점 포착, 주문 체결 정산 결과, 그리고 **매일 자정 지난 24시간 동안의 가중 평단가 기반 승률(Win Rate) 및 PnL**을 정밀 집계하여 프리미엄 HTML 리포트 형태로 스마트폰 관제탑에 브리핑합니다.

특히 **v9.3 업데이트**를 통해 실시간 상태 모니터링과 일일 결산 리포트의 신뢰성을 극적으로 개선했습니다:

### 1. 🔍 다중 레이어 포지션 크로스 밸리데이션 (Cross-Validation)
- **CCXT & DB 이중 검증**: 일일 결산 리포트 생성 시, 로컬 DB 상태에만 의존하지 않고 CCXT API `fetch_positions`를 통해 바이낸스 선물 계정의 실제 활성 포지션을 실시간으로 직접 대조 검증합니다.
- **SHORT(숏) 포지션 완벽 지원**: 기존 롱(LONG) 중심 정산 아키텍처를 전면 개편하여, 숏 포지션이 유지 중일 때 FLAT으로 잘못 표기되던 집계 누락을 완벽히 해결했습니다.

### 2. 🧮 정밀화된 PnL 연산 및 미실현 손익(Unrealized PnL) 추적
- **방향성별 실현 PnL 공식 분리**: 숏 포지션 청산 시 실현 PnL을 `(평균매도가 - 매수단가) * 수량`으로 정확히 계산하도록 수식을 정교화했습니다.
- **미실현 손익 실시간 반영**: 리포트 발행 시점 기준으로 바이낸스에 활성 포지션이 유지 중일 경우, 실시간 평가 손익(Unrealized PnL)을 리포트에 함께 표기하여 계좌 상태의 가시성을 극대화합니다.

### ⏱️ 3. Telegram `/status` 브리핑 및 보유 시간(Duration) 정산 개선
- active side(`BUY`/`SELL`)에 대응하는 정밀 트레이드 이력을 추적하여, 텔레그램 `/status` 호출 시 포지션 진입 시점과 경과 시간(`pos_duration_str`)이 누락 없이 실시간으로 계산 및 정산되도록 개선했습니다.

---

## 💻 Installation & Quick Start

본 프로젝트는 도커 오케스트레이션으로 100% 캡슐화되어 있어, 환경변수 설정 후 단 한 줄의 명령어로 클린 빌드가 가능합니다.

```bash
# 1. 환경 변수 샘플 카피 및 설정
cp .env.example .env

# 2. 인프라 전체 클린 리빌드 및 백그라운드 가동
docker compose up -d --build

# 3. 실시간 분산 워커 파이프라인 로그 모니터링
docker compose logs -f worker
```
