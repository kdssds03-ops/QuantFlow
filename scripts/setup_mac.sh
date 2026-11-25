#!/usr/bin/env bash
# macOS(Apple Silicon) 로컬 개발환경 셋업 — Docker 없이 스크립트/백테스트/테스트 실행용.
# 사용: bash scripts/setup_mac.sh
set -euo pipefail
cd "$(dirname "$0")/.."

command -v uv >/dev/null || { echo "uv 필요: curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }

# Dockerfile과 동일한 Python 3.12
[ -d .venv ] || uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt

# LightGBM/XGBoost는 libomp(보통 Homebrew)가 필요. Homebrew 없이도 되도록
# scikit-learn 휠에 동봉된 libomp를 각 라이브러리 옆에 두고 rpath를 추가한다.
SP=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
for pair in "lightgbm:lib_lightgbm.dylib" "xgboost:libxgboost.dylib"; do
  pkg=${pair%%:*}; lib="$SP/$pkg/lib/${pair#*:}"
  .venv/bin/python -c "import $pkg" 2>/dev/null && continue
  cp "$SP/sklearn/.dylibs/libomp.dylib" "$SP/$pkg/lib/libomp.dylib"
  install_name_tool -add_rpath @loader_path "$lib" 2>/dev/null || true
  codesign --force -s - "$lib" "$SP/$pkg/lib/libomp.dylib"
done

.venv/bin/python -c 'import lightgbm, xgboost, ccxt, celery, fastapi, ta; print("imports OK")'
[ -f .env ] || { cp .env.example .env; echo ".env 생성됨 (EXCHANGE_SANDBOX=true 기본). 키는 직접 채우세요."; }
echo "완료: source .venv/bin/activate && export PYTHONPATH=\$PWD"
