#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$SCRIPT_DIR/.venv/bin/python}"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-$SCRIPT_DIR/.runtime.env}"
HOST="${HOST:-}"
MYSQLSH_BINARY="${MYSQLSH_BINARY:-}"

if [[ -f "$RUNTIME_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$RUNTIME_ENV_FILE"
fi

HOST="${HOST:-0.0.0.0}"
DEFAULT_HTTP_PORT="${DEFAULT_HTTP_PORT:-80}"
PORT="${PORT:-$DEFAULT_HTTP_PORT}"
export HOST PORT MYSQLSH_BINARY

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python runtime not found at $PYTHON_BIN. Run ./setup.sh first or set PYTHON_BIN." >&2
  exit 1
fi

cd "$SCRIPT_DIR"
exec "$PYTHON_BIN" - <<'PY'
import os

import app as module

module.ensure_profile_store()
module.ensure_object_storage_store()
module.ensure_par_store()
module.ensure_runtime_dirs()
module.app.run(
    debug=False,
    host=os.environ.get("HOST", "0.0.0.0"),
    port=int(os.environ.get("PORT", "80")),
)
PY
