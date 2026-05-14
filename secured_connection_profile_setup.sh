#!/usr/bin/env bash
set -euo pipefail

PROFILE_STORE="${PROFILE_STORE:-profiles.json}"
SSH_KEY_DIR="${SSH_KEY_DIR:-profile_ssh_keys}"
LOCAL_PROFILE_NAME="${LOCAL_PROFILE_NAME:-local-admin-profile}"
LOCAL_MYSQL_SOCKET="${LOCAL_MYSQL_SOCKET:-.data/run/mysql.sock}"
LOCAL_MYSQL_ADMIN_USER="${LOCAL_MYSQL_ADMIN_USER:-localadmin}"
LOCAL_MYSQL_DATABASE="${LOCAL_MYSQL_DATABASE:-mysql}"
FORCE_PASSWORD_CHANGE="${FORCE_PASSWORD_CHANGE:-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

usage() {
  cat <<'USAGE'
Usage: secured_connection_profile_setup.sh [options]

Options:
  --profile-store PATH
  --ssh-key-dir PATH
  --profile-name NAME
  --socket PATH
  --admin-user USER
  --database NAME
  --no-force-password-change
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile-store) PROFILE_STORE="${2:?Missing value for --profile-store}"; shift 2 ;;
    --ssh-key-dir) SSH_KEY_DIR="${2:?Missing value for --ssh-key-dir}"; shift 2 ;;
    --profile-name) LOCAL_PROFILE_NAME="${2:?Missing value for --profile-name}"; shift 2 ;;
    --socket) LOCAL_MYSQL_SOCKET="${2:?Missing value for --socket}"; shift 2 ;;
    --admin-user) LOCAL_MYSQL_ADMIN_USER="${2:?Missing value for --admin-user}"; shift 2 ;;
    --database) LOCAL_MYSQL_DATABASE="${2:?Missing value for --database}"; shift 2 ;;
    --no-force-password-change) FORCE_PASSWORD_CHANGE="0"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

require_safe_profile_name() {
  local value="$1"
  if [[ ! "$value" =~ ^[A-Za-z0-9_.-]+$ ]]; then
    echo "Profile name must contain only letters, numbers, dot, underscore, or hyphen." >&2
    exit 2
  fi
}

require_repo_local_path() {
  local label="$1"
  local path_value="$2"
  local resolved_parent
  local resolved_path
  mkdir -p "$(dirname "$path_value")"
  resolved_parent="$(cd "$(dirname "$path_value")" && pwd -P)"
  resolved_path="$resolved_parent/$(basename "$path_value")"
  case "$resolved_path" in
    "$PWD"/*) ;;
    *)
      echo "$label must resolve under the application directory: $path_value" >&2
      exit 2
      ;;
  esac
}

require_safe_profile_name "$LOCAL_PROFILE_NAME"
require_repo_local_path "PROFILE_STORE" "$PROFILE_STORE"
require_repo_local_path "SSH_KEY_DIR" "$SSH_KEY_DIR"
mkdir -p "$(dirname "$PROFILE_STORE")" "$SSH_KEY_DIR"
chmod 0700 "$SSH_KEY_DIR"

"$PYTHON_BIN" - "$PROFILE_STORE" "$LOCAL_PROFILE_NAME" "$LOCAL_MYSQL_SOCKET" "$LOCAL_MYSQL_ADMIN_USER" "$LOCAL_MYSQL_DATABASE" "$FORCE_PASSWORD_CHANGE" <<'PY'
import json
import os
import sys
from pathlib import Path

profile_store = Path(sys.argv[1])
profile_name = sys.argv[2]
socket_path = sys.argv[3]
admin_user = sys.argv[4]
database = sys.argv[5]
force_password_change = sys.argv[6].lower() not in {"0", "false", "no", "off"}
secret_keys = {"password", "passwd", "secret", "token", "private_key", "private_key_content", "private_key_passphrase", "dsn", "uri"}

payload = {}
if profile_store.exists():
    try:
        payload = json.loads(profile_store.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Unable to parse {profile_store}: {exc}")

rows = payload.get("profiles", []) if isinstance(payload, dict) else []
profiles = {}
for row in rows:
    if not isinstance(row, dict):
        continue
    name = str(row.get("name", "")).strip()
    if not name:
        continue
    clean = {key: value for key, value in row.items() if key.lower() not in secret_keys}
    if name != profile_name:
        clean["profile_management"] = False
    profiles[name] = clean

profiles[profile_name] = {
    "name": profile_name,
    "mode": "socket",
    "host": "",
    "port": 3306,
    "socket": socket_path,
    "database": database,
    "default_username": admin_user,
    "profile_management": True,
    "force_password_change": force_password_change,
    "ssh_enabled": False,
    "ssh_key_uploaded": False,
    "ssh_key_id": "",
    "ssh_host": "",
    "ssh_port": 22,
    "ssh_user": "",
    "ssh_key_path": "",
    "ssh_config_file": "",
}

next_payload = {"profiles": [profiles[name] for name in sorted(profiles, key=str.lower)]}
profile_store.parent.mkdir(parents=True, exist_ok=True)
temp_path = profile_store.with_suffix(profile_store.suffix + ".tmp")
temp_path.write_text(json.dumps(next_payload, indent=2) + "\n", encoding="utf-8")
os.chmod(temp_path, 0o600)
temp_path.replace(profile_store)
os.chmod(profile_store, 0o600)
PY

find "$SSH_KEY_DIR" -type d -exec chmod 0700 {} \; 2>/dev/null || true
find "$SSH_KEY_DIR" -type f -exec chmod 0600 {} \; 2>/dev/null || true
echo "Secured local admin profile metadata refreshed at $PROFILE_STORE."
