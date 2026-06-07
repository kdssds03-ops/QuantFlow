# QuantFlow 배포 가이드 — Oracle Cloud 무료 등급 (ARM / Ubuntu)

24시간 무중단 가동을 위해 봇을 노트북에서 **Oracle Cloud Always Free ARM VM**으로 이전한다.
스택(postgres/redis/api/worker/beat/listener-worker)은 ARM64에서 코드 수정 없이 구동됨(검증 완료).

---

## 0. 사전 준비
- Oracle Cloud 계정(신용/체크카드 본인확인 필요 — 과금 아님, 무료 등급 유지)
- 로컬에 SSH 키페어 (`ssh-keygen -t ed25519`) — 공개키를 인스턴스 생성 시 등록

## 1. Always Free ARM 인스턴스 생성
- **Compute → Instances → Create Instance**
- **Image**: Canonical Ubuntu 22.04
- **Shape**: `VM.Standard.A1.Flex` (Ampere ARM) — **무료 한도: 합계 4 OCPU / 24GB RAM**. 권장 **2 OCPU / 12GB** (넉넉).
- **Region/AD**: 도쿄(`ap-tokyo-1`) 또는 서울(`ap-seoul-1`) — Binance 호환·지연 유리. ⚠️ **미국 리전 금지**(Binance 차단 위험).
- SSH 공개키 등록 → 생성.

### ⚠️ Oracle 최대 함정: "Out of host capacity"
무료 ARM은 인기가 많아 생성 시 용량부족 에러가 흔하다. 대응:
- 다른 **가용 도메인(AD-1/2/3)** 으로 재시도
- 시간대를 바꿔 재시도 / 또는 생성 스크립트로 자동 재시도
- 정 안되면 다른 무료 리전(서울↔도쿄) 시도

## 2. 접속 & 시스템 준비
```bash
ssh ubuntu@<인스턴스_공인IP>
sudo apt update && sudo apt -y upgrade
sudo timedatectl set-timezone Asia/Seoul   # 로그/결산 KST 정렬
```

## 3. Docker + Compose 설치 (ARM64)
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu      # 재로그인 후 sudo 없이 docker 사용
sudo systemctl enable --now docker
docker compose version              # plugin 포함 확인
```

## 4. 코드 배포
```bash
git clone <레포_URL> quantflow && cd quantflow
cp .env.example .env
nano .env       # 아래 5번 값 채우기
docker compose up -d   # 최초 빌드(ARM 컴파일로 수 분 소요) 후 가동
```

## 5. .env 핵심 설정 (실전 기준)
```
EXCHANGE_SANDBOX=false              # 데모로 먼저 검증하려면 true 유지
EXCHANGE_API_KEY=<실계정 키>
EXCHANGE_API_SECRET=<실계정 시크릿>
PREDICTOR_TYPE=TREND
RISK_FACTOR=0.05                    # 처음엔 보수적으로
VOL_TARGET_ENABLED=true
MAX_DAILY_LOSS_PCT=0.05             # 서킷브레이커
TELEGRAM_BOT_TOKEN=<토큰>
TELEGRAM_CHAT_ID=<챗ID>
SECRET_KEY=<랜덤 32자>
# 데모용 BINANCE_URLS_API_FUTURES 줄은 실전 시 삭제
```

## 6. 보안 (실자금 — 필수)
- **거래소 API 키**: 출금 권한 **OFF**, **이 인스턴스 공인 IP를 화이트리스트**.
- **방화벽 2중**(Oracle은 클라우드 방화벽 + OS 방화벽 둘 다 존재):
  - OCI 콘솔 **Security List/NSG**: 인바운드 **22(SSH)만** 허용. 8000/5432/6379 외부 개방 금지.
  - OS: `sudo ufw allow 22/tcp && sudo ufw enable` (또는 Oracle 기본 iptables 유지).
  - API(8000)·DB(5432)·Redis(6379)는 **컨테이너 내부 통신만** — 절대 공개 금지.
- `.env`는 git에 올리지 않음(이미 gitignore). 키 노출 주의.

## 7. 가동 검증
```bash
docker compose ps                      # 6개 컨테이너 Up 확인
python scripts/preflight.py            # One-Way 모드·레버리지·DB·API키 점검 (FAIL 0 목표)
docker compose logs -f worker          # TREND 신호·매매 로그 관찰
```
- 거래소에서 **One-Way 포지션 모드** + **레버리지 1~3x** 설정(봇은 안 건드림).
- TREND은 4h봉 ~62개(≈10일 1m) 필요 → 부족하면 `python scripts/backfill_db.py`로 백필.

## 8. 운영 메모
- **자동 재시작**: `restart: unless-stopped` 설정돼 있어 인스턴스 재부팅 시 자동 복구.
- **코드 변경 반영**: `git pull && docker compose restart <svc>` (PYTHONDONTWRITEBYTECODE 적용으로 restart면 충분).
- **.env 변경 반영**: `docker compose up -d <svc>` (restart 아님).
- **로그 비대 방지**(선택): docker-compose 각 서비스에 logging 옵션 추가
  ```yaml
  logging: { driver: json-file, options: { max-size: "10m", max-file: "5" } }
  ```
- **원격 제어**: 텔레그램 `/status`, `/pause`, `/resume` — 노트북 없이 어디서든 가능.
- **모니터링**: Claude 앱 의존 헬스체크 대신, 봇 내장 헬스체크(Celery beat + 텔레그램)를 쓰면 서버에서 24h 자가감시 가능(추가 구현 옵션).

## 부록: ARM 확인 결과
모든 의존성(lightgbm 4.x, xgboost 2.x, scikit-learn, pandas, numpy, asyncpg, psycopg2-binary 등)이 aarch64 휠 제공 또는 소스 빌드 가능(build-essential 포함). numba는 미사용. → **ARM 전용 수정 불필요.**
