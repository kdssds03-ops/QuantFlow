# 트레이딩 도구 설치

2026-11-18에 아래 저장소를 `external/`에 설치했습니다. 각 Python 프로그램은 별도 Python 3.12 가상환경을 사용합니다. 설치 소스와 가상환경은 Git 및 Docker 빌드 대상에서 제외됩니다. 커밋은 `external/installed-revisions.json`에 기록했습니다.

## Day Trader Skill

원본: https://github.com/Jakeschincariol/day-trader-skill

11개 `trader-*` 스킬을 `/Users/kimhyunmin/.codex/skills/`에 설치했습니다. 다음 대화 턴부터 사용할 수 있습니다. Claude 전용 경로 변수는 실제 설치 경로로 치환했습니다.

예: “trader-scanner로 시장을 스캔해줘”. SEC 공시 기능에는 본인 이름과 연락처를 담은 `SEC_USER_AGENT` 설정이 필요합니다.

## TradingAgents 0.7.1

원본: https://github.com/TauricResearch/TradingAgents

```bash
cd /Users/kimhyunmin/Desktop/quantflow/external/TradingAgents
.venv/bin/tradingagents
```

실제 분석 전 이 폴더의 `.env`에 사용할 LLM 제공자의 API 키를 입력하세요. 원본 `.env.example`도 보존되어 있습니다. 다른 인증 방식을 쓰려면 원본 README의 안내를 따르세요.

## Alpaca MCP Server 2.4.1

원본: https://github.com/alpacahq/alpaca-mcp-server

Codex 사용자 설정 `/Users/kimhyunmin/.codex/config.toml`에 `alpaca`라는 이름으로 등록했습니다. 자격 증명이 없어 현재 `enabled = false` 상태입니다.

1. `/Users/kimhyunmin/Desktop/quantflow/external/alpaca-mcp-server/.env`에 모의투자 계정의 `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`를 입력합니다.
2. Codex 설정의 `[mcp_servers.alpaca]` 아래 `enabled = false`를 `enabled = true`로 변경합니다.
3. Codex를 재시작하여 MCP 연결을 다시 로드합니다.

`ALPACA_PAPER_TRADE=true`가 MCP 실행 환경에 명시되어 있습니다. 로컬 stdio 연결이며 공개 HTTP 서버를 실행하지 않습니다. API 키 파일 권한은 소유자만 읽고 쓸 수 있는 0600입니다.

이 설치 과정에서는 실제 계정 연결, LLM 분석 요청, 주문 실행을 하지 않았습니다.
