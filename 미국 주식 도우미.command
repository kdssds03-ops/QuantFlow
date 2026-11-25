#!/bin/zsh
# 미국 주식 개장 범위 돌파 감시(60초마다 갱신). 종료: Ctrl+C
cd "${0:A:h}" || exit 1
exec .venv/bin/python -m scripts.stock_advisor --watch
