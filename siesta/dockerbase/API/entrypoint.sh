#!/bin/bash
set -euo pipefail

PROJECT_ROOT="${SIESTA_PROJECT_ROOT:-/workspace}"
VENV_DIR="${SIESTA_VENV_DIR:-${PROJECT_ROOT}/.venv}"
FALLBACK_VENV_DIR="${SIESTA_FALLBACK_VENV_DIR:-/tmp/siesta-api-venv}"
PYTHON_BIN="${SIESTA_PYTHON_BIN:-/opt/bitnami/python/bin/python3}"
INSTALL_SCRIPT="${SIESTA_INSTALL_SCRIPT:-${PROJECT_ROOT}/siesta/install_dependencies.py}"
ENTRYPOINT_SCRIPT="${SIESTA_ENTRYPOINT_SCRIPT:-${PROJECT_ROOT}/main.py}"
DEFAULT_CONFIG_PATH="${SIESTA_DEFAULT_CONFIG_PATH:-${PROJECT_ROOT}/config/siesta.config.json}"
CONFIG_PATH="${SIESTA_CONFIG:-}"

# Some Java/Hadoop auth paths rely on USER/LOGNAME being present.
export USER="${USER:-spark}"
export LOGNAME="${LOGNAME:-$USER}"

cd "$PROJECT_ROOT"


python -m pip install --upgrade pip
python "$INSTALL_SCRIPT"

if [[ -z "$CONFIG_PATH" && -f "$DEFAULT_CONFIG_PATH" ]]; then
    CONFIG_PATH="$DEFAULT_CONFIG_PATH"
fi

if [[ -n "$CONFIG_PATH" && -f "$CONFIG_PATH" ]]; then
    echo "Starting Siesta API with config: $CONFIG_PATH"
    exec python "$ENTRYPOINT_SCRIPT" --config "$CONFIG_PATH"
fi

echo "Starting Siesta API with default configuration"
exec python "$ENTRYPOINT_SCRIPT"