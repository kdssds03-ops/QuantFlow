#!/bin/zsh
# 주식 나침반(국내 주식 리스크·비중 관리) 열기. 이미 켜져 있으면 브라우저만 연다.
cd "${0:A:h}" || exit 1
if /usr/bin/curl -fsS --max-time 2 http://127.0.0.1:8765/ | /usr/bin/grep -q '<title>주식 나침반'; then
    open http://127.0.0.1:8765/
else
    exec .venv/bin/python -m scripts.kr_compass --open
fi
