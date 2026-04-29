#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/.venv}"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-$SCRIPT_DIR/.runtime.env}"
OS_FAMILY_INPUT="${OS_FAMILY:-}"
DEPLOY_MODE_INPUT="${DEPLOY_MODE:-}"
HTTP_PORT_INPUT="${HTTP_PORT:-}"
HTTPS_PORT_INPUT="${HTTPS_PORT:-}"
HOST_INPUT="${HOST:-}"
SSL_CERT_FILE_INPUT="${SSL_CERT_FILE:-}"
SSL_KEY_FILE_INPUT="${SSL_KEY_FILE:-}"
SERVICE_USER_INPUT="${SERVICE_USER:-}"
SERVICE_GROUP_INPUT="${SERVICE_GROUP:-}"
EXISTING_DEFAULT_HTTP_PORT=""
EXISTING_DEFAULT_HTTPS_PORT=""
EXISTING_HOST=""
EXISTING_SSL_CERT_FILE=""
EXISTING_SSL_KEY_FILE=""
APP_SLUG="mysql-shell-web"
APP_NAME="MySQL Shell Web"

print_usage() {
  cat <<EOF
Usage:
  ./setup.sh [os_family] [deploy_mode] [http_port] [https_port]
  ./setup.sh [os_family] [deploy_mode] [--http-port PORT] [--https-port PORT]

Arguments:
  os_family    ol8 | ol9 | ubuntu | macos
  deploy_mode  http | https | both | none

Environment overrides:
  OS_FAMILY, DEPLOY_MODE, HOST, HTTP_PORT, HTTPS_PORT, SSL_CERT_FILE,
  SSL_KEY_FILE, SERVICE_USER, SERVICE_GROUP, VENV_DIR, RUNTIME_ENV_FILE
EOF
}

is_interactive_terminal() {
  [[ -t 0 && -t 1 ]]
}

parse_args() {
  local positional=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h|--help)
        print_usage
        exit 0
        ;;
      --http-port)
        if [[ $# -lt 2 ]]; then
          echo "--http-port requires a port value." >&2
          return 1
        fi
        HTTP_PORT_INPUT="$2"
        shift 2
        ;;
      --https-port)
        if [[ $# -lt 2 ]]; then
          echo "--https-port requires a port value." >&2
          return 1
        fi
        HTTPS_PORT_INPUT="$2"
        shift 2
        ;;
      --)
        shift
        while [[ $# -gt 0 ]]; do
          positional+=("$1")
          shift
        done
        ;;
      -*)
        echo "Unknown option: $1" >&2
        return 1
        ;;
      *)
        positional+=("$1")
        shift
        ;;
    esac
  done

  case "${#positional[@]}" in
    0) ;;
    1)
      OS_FAMILY_INPUT="${positional[0]}"
      ;;
    2)
      OS_FAMILY_INPUT="${positional[0]}"
      DEPLOY_MODE_INPUT="${positional[1]}"
      ;;
    3)
      OS_FAMILY_INPUT="${positional[0]}"
      DEPLOY_MODE_INPUT="${positional[1]}"
      HTTP_PORT_INPUT="${positional[2]}"
      ;;
    4)
      OS_FAMILY_INPUT="${positional[0]}"
      DEPLOY_MODE_INPUT="${positional[1]}"
      HTTP_PORT_INPUT="${positional[2]}"
      HTTPS_PORT_INPUT="${positional[3]}"
      ;;
    *)
      echo "Too many positional arguments." >&2
      print_usage >&2
      return 1
      ;;
  esac
}

to_lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

normalize_os_family() {
  case "$(to_lower "$1")" in
    ol8|oraclelinux8|oracle-linux-8) echo "ol8" ;;
    ol9|oraclelinux9|oracle-linux-9) echo "ol9" ;;
    ubuntu) echo "ubuntu" ;;
    macos|mac|darwin|osx) echo "macos" ;;
    *)
      echo "Unsupported OS family '$1'. Use one of: ol8, ol9, ubuntu, macos." >&2
      return 1
      ;;
  esac
}

detect_os_family() {
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "macos"
    return 0
  fi

  if [[ ! -r /etc/os-release ]]; then
    echo "Unable to detect the operating system. Pass one of: ol8, ol9, ubuntu, macos." >&2
    return 1
  fi

  # shellcheck disable=SC1091
  source /etc/os-release
  case "$(to_lower "${ID:-unknown}"):${VERSION_ID%%.*}" in
    ol:8|oraclelinux:8) echo "ol8" ;;
    ol:9|oraclelinux:9) echo "ol9" ;;
    ubuntu:*) echo "ubuntu" ;;
    *)
      echo "Unsupported operating system: ${ID:-unknown} ${VERSION_ID:-unknown}. Pass one of: ol8, ol9, ubuntu, macos." >&2
      return 1
      ;;
  esac
}

normalize_deploy_mode() {
  local normalized
  normalized="$(to_lower "$1")"
  case "$normalized" in
    http|https|both|none) echo "$normalized" ;;
    *)
      echo "Unsupported deploy mode '$1'. Use http, https, both, or none." >&2
      return 1
      ;;
  esac
}

normalize_port() {
  local label="$1"
  local port_value="$2"

  if [[ ! "$port_value" =~ ^[0-9]+$ ]]; then
    echo "${label} port must be numeric. Received '$port_value'." >&2
    return 1
  fi

  if (( port_value < 1 || port_value > 65535 )); then
    echo "${label} port must be between 1 and 65535. Received '$port_value'." >&2
    return 1
  fi

  echo "$port_value"
}

load_existing_runtime_env() {
  if [[ ! -f "$RUNTIME_ENV_FILE" ]]; then
    return 0
  fi

  unset DEFAULT_HTTP_PORT DEFAULT_HTTPS_PORT HOST SSL_CERT_FILE SSL_KEY_FILE
  # shellcheck disable=SC1090
  source "$RUNTIME_ENV_FILE"
  EXISTING_DEFAULT_HTTP_PORT="${DEFAULT_HTTP_PORT:-}"
  EXISTING_DEFAULT_HTTPS_PORT="${DEFAULT_HTTPS_PORT:-}"
  EXISTING_HOST="${HOST:-}"
  EXISTING_SSL_CERT_FILE="${SSL_CERT_FILE:-}"
  EXISTING_SSL_KEY_FILE="${SSL_KEY_FILE:-}"
}

resolve_value() {
  local provided="$1"
  local existing="$2"
  local fallback="$3"

  if [[ -n "$provided" ]]; then
    echo "$provided"
  elif [[ -n "$existing" ]]; then
    echo "$existing"
  else
    echo "$fallback"
  fi
}

display_prompt_value() {
  local value="$1"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '<empty>'
  fi
}

prompt_for_normalized_value() {
  local label="$1"
  local current_value="$2"
  local normalizer="$3"
  local help_text="$4"
  local entered_value
  local normalized_value

  while true; do
    printf '%s [%s]: ' "$label" "$(display_prompt_value "$current_value")" >&2
    if ! read -r entered_value; then
      echo >&2
      echo "$current_value"
      return 0
    fi
    if [[ -z "$entered_value" ]]; then
      echo "$current_value"
      return 0
    fi

    if normalized_value="$("$normalizer" "$entered_value" 2>/dev/null)"; then
      echo "$normalized_value"
      return 0
    fi

    echo "$help_text" >&2
  done
}

prompt_for_text_value() {
  local label="$1"
  local current_value="$2"
  local allow_empty="$3"
  local entered_value

  while true; do
    printf '%s [%s]: ' "$label" "$(display_prompt_value "$current_value")" >&2
    if ! read -r entered_value; then
      echo >&2
      echo "$current_value"
      return 0
    fi
    if [[ -z "$entered_value" ]]; then
      if [[ "$allow_empty" == "yes" || -n "$current_value" ]]; then
        echo "$current_value"
        return 0
      fi
      echo "$label cannot be empty." >&2
      continue
    fi

    echo "$entered_value"
    return 0
  done
}

prompt_for_port_value() {
  local label="$1"
  local current_value="$2"
  local entered_value
  local normalized_value

  while true; do
    printf '%s port [%s]: ' "$label" "$current_value" >&2
    if ! read -r entered_value; then
      echo >&2
      echo "$current_value"
      return 0
    fi
    if [[ -z "$entered_value" ]]; then
      echo "$current_value"
      return 0
    fi

    if normalized_value="$(normalize_port "$label" "$entered_value" 2>/dev/null)"; then
      echo "$normalized_value"
      return 0
    fi

    echo "Enter a numeric port between 1 and 65535, or press Enter to keep $current_value." >&2
  done
}

prompt_for_ports_if_needed() {
  local deploy_mode="$1"
  local http_port="$2"
  local https_port="$3"

  if ! is_interactive_terminal; then
    printf '%s\n%s\n' "$http_port" "$https_port"
    return 0
  fi

  case "$deploy_mode" in
    http)
      if [[ -z "$HTTP_PORT_INPUT" ]]; then
        echo "Press Enter to keep the current HTTP port." >&2
        http_port="$(prompt_for_port_value "HTTP" "$http_port")"
      fi
      ;;
    https)
      if [[ -z "$HTTPS_PORT_INPUT" ]]; then
        echo "Press Enter to keep the current HTTPS port." >&2
        https_port="$(prompt_for_port_value "HTTPS" "$https_port")"
      fi
      ;;
    both)
      if [[ -z "$HTTP_PORT_INPUT" || -z "$HTTPS_PORT_INPUT" ]]; then
        echo "Press Enter to keep the current port values." >&2
      fi
      if [[ -z "$HTTP_PORT_INPUT" ]]; then
        http_port="$(prompt_for_port_value "HTTP" "$http_port")"
      fi
      if [[ -z "$HTTPS_PORT_INPUT" ]]; then
        https_port="$(prompt_for_port_value "HTTPS" "$https_port")"
      fi
      ;;
    none)
      echo "Deploy mode is 'none'; keeping saved HTTP and HTTPS port defaults." >&2
      ;;
  esac

  printf '%s\n%s\n' "$http_port" "$https_port"
}

open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "macOS does not expose Linux-style port opening here. Allow the Python process through the macOS firewall if prompted, or open ${port_value}/tcp for ${protocol_label} manually." >&2
    return 0
  fi

  if command -v firewall-cmd >/dev/null 2>&1; then
    sudo firewall-cmd --permanent --add-port="${port_value}/tcp"
    sudo firewall-cmd --reload
    echo "Opened firewall port ${port_value}/tcp for ${protocol_label} with firewall-cmd."
    return 0
  fi

  if command -v ufw >/dev/null 2>&1; then
    sudo ufw allow "${port_value}/tcp"
    echo "Opened firewall port ${port_value}/tcp for ${protocol_label} with ufw."
    return 0
  fi

  echo "Firewall tool not found. Open ${port_value}/tcp for ${protocol_label} manually on this host." >&2
}

write_runtime_env() {
  local http_port="$1"
  local https_port="$2"
  local host_value="$3"
  local ssl_cert_file="$4"
  local ssl_key_file="$5"

  {
    echo "# Generated by setup.sh"
    echo "HOST=$host_value"
    echo "DEFAULT_HTTP_PORT=$http_port"
    echo "DEFAULT_HTTPS_PORT=$https_port"
    if [[ -n "$ssl_cert_file" ]]; then
      echo "SSL_CERT_FILE=$ssl_cert_file"
    else
      echo "# SSL_CERT_FILE=/path/to/cert.pem"
    fi
    if [[ -n "$ssl_key_file" ]]; then
      echo "SSL_KEY_FILE=$ssl_key_file"
    else
      echo "# SSL_KEY_FILE=/path/to/key.pem"
    fi
  } >"$RUNTIME_ENV_FILE"
}

fix_tls_permissions() {
  local ssl_cert_file="$1"
  local ssl_key_file="$2"
  local service_user="$3"
  local service_group="$4"

  chmod 644 "$ssl_cert_file"
  chmod 600 "$ssl_key_file"

  if [[ -n "$service_user" && -n "$service_group" ]]; then
    sudo chown "$service_user:$service_group" "$ssl_cert_file" "$ssl_key_file"
  fi
}

generate_self_signed_tls_assets() {
  local host_value="$1"
  local ssl_cert_file="$2"
  local ssl_key_file="$3"
  local service_user="$4"
  local service_group="$5"
  local common_name="localhost"
  local tls_dir

  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate a default TLS certificate. Install openssl or provide SSL_CERT_FILE and SSL_KEY_FILE." >&2
    return 1
  fi

  if [[ -n "$host_value" && "$host_value" != "0.0.0.0" && "$host_value" != "::" ]]; then
    common_name="$host_value"
  fi

  tls_dir="$(dirname "$ssl_cert_file")"
  mkdir -p "$tls_dir"

  openssl req \
    -x509 \
    -nodes \
    -newkey rsa:2048 \
    -days 365 \
    -keyout "$ssl_key_file" \
    -out "$ssl_cert_file" \
    -subj "/CN=$common_name" >/dev/null 2>&1

  fix_tls_permissions "$ssl_cert_file" "$ssl_key_file" "$service_user" "$service_group"
  echo "Generated self-signed TLS certificate: $ssl_cert_file" >&2
}

ensure_https_tls_assets() {
  local deploy_mode="$1"
  local host_value="$2"
  local ssl_cert_file="$3"
  local ssl_key_file="$4"
  local service_user="$5"
  local service_group="$6"
  local default_tls_dir="$SCRIPT_DIR/tls"

  if [[ "$deploy_mode" != "https" && "$deploy_mode" != "both" ]]; then
    printf '%s\n%s\n' "$ssl_cert_file" "$ssl_key_file"
    return 0
  fi

  if [[ -n "$ssl_cert_file" || -n "$ssl_key_file" ]]; then
    printf '%s\n%s\n' "$ssl_cert_file" "$ssl_key_file"
    return 0
  fi

  ssl_cert_file="$default_tls_dir/${APP_SLUG}-selfsigned.crt"
  ssl_key_file="$default_tls_dir/${APP_SLUG}-selfsigned.key"

  if [[ ! -f "$ssl_cert_file" || ! -f "$ssl_key_file" ]]; then
    generate_self_signed_tls_assets "$host_value" "$ssl_cert_file" "$ssl_key_file" "$service_user" "$service_group" || return 1
  else
    fix_tls_permissions "$ssl_cert_file" "$ssl_key_file" "$service_user" "$service_group"
    echo "Reusing self-signed TLS certificate: $ssl_cert_file" >&2
  fi

  printf '%s\n%s\n' "$ssl_cert_file" "$ssl_key_file"
}

resolve_service_user() {
  if [[ -n "$SERVICE_USER_INPUT" ]]; then
    echo "$SERVICE_USER_INPUT"
  elif [[ -n "${SUDO_USER:-}" ]]; then
    echo "$SUDO_USER"
  else
    id -un
  fi
}

resolve_service_group() {
  local service_user="$1"

  if [[ -n "$SERVICE_GROUP_INPUT" ]]; then
    echo "$SERVICE_GROUP_INPUT"
  else
    id -gn "$service_user"
  fi
}

resolve_bash_bin() {
  local bash_bin

  bash_bin="$(command -v bash || true)"
  if [[ -z "$bash_bin" ]]; then
    echo "bash is required but was not found in PATH." >&2
    return 1
  fi

  printf '%s\n' "$bash_bin"
}

install_systemd_service() {
  local service_name="$1"
  local description="$2"
  local exec_script="$3"
  local service_user="$4"
  local service_group="$5"
  local unit_path="/etc/systemd/system/${service_name}.service"
  local bash_bin

  bash_bin="$(resolve_bash_bin)" || return 1

  sudo tee "$unit_path" >/dev/null <<EOF
[Unit]
Description=$description
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_group
WorkingDirectory=$SCRIPT_DIR
EnvironmentFile=-$RUNTIME_ENV_FILE
ExecStart=$bash_bin $exec_script
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
}

enable_systemd_service() {
  local service_name="$1"

  sudo systemctl enable --now "${service_name}.service"
  echo "Enabled systemd service ${service_name}.service."
}

disable_systemd_service() {
  local service_name="$1"

  sudo systemctl disable --now "${service_name}.service" >/dev/null 2>&1 || true
}

https_service_ready() {
  local ssl_cert_file="$1"
  local ssl_key_file="$2"

  if [[ -z "$ssl_cert_file" || -z "$ssl_key_file" ]]; then
    echo "HTTPS service was installed but not started because SSL_CERT_FILE and SSL_KEY_FILE are not set in $RUNTIME_ENV_FILE." >&2
    return 1
  fi

  if [[ ! -f "$ssl_cert_file" || ! -f "$ssl_key_file" ]]; then
    echo "HTTPS service was installed but not started because the TLS certificate or key file does not exist." >&2
    return 1
  fi

  return 0
}

setup_systemd_services() {
  local os_family="$1"
  local deploy_mode="$2"
  local ssl_cert_file="$3"
  local ssl_key_file="$4"
  local service_user
  local service_group
  local http_service="${APP_SLUG}-http"
  local https_service="${APP_SLUG}-https"

  case "$os_family" in
    ol8|ol9|ubuntu) ;;
    *)
      return 0
      ;;
  esac

  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl was not found. Create the service manually if you need background startup on this host." >&2
    return 0
  fi

  service_user="$(resolve_service_user)"
  service_group="$(resolve_service_group "$service_user")"

  install_systemd_service "$http_service" "$APP_NAME HTTP service" "$SCRIPT_DIR/start_http.sh" "$service_user" "$service_group"
  install_systemd_service "$https_service" "$APP_NAME HTTPS service" "$SCRIPT_DIR/start_https.sh" "$service_user" "$service_group"
  sudo systemctl daemon-reload
  echo "Installed systemd unit files for $APP_NAME."

  case "$deploy_mode" in
    http)
      enable_systemd_service "$http_service"
      disable_systemd_service "$https_service"
      ;;
    https)
      disable_systemd_service "$http_service"
      if https_service_ready "$ssl_cert_file" "$ssl_key_file"; then
        enable_systemd_service "$https_service"
      else
        disable_systemd_service "$https_service"
      fi
      ;;
    both)
      enable_systemd_service "$http_service"
      if https_service_ready "$ssl_cert_file" "$ssl_key_file"; then
        enable_systemd_service "$https_service"
      else
        disable_systemd_service "$https_service"
      fi
      ;;
    none)
      disable_systemd_service "$http_service"
      disable_systemd_service "$https_service"
      echo "Installed systemd units but left them disabled because deploy mode is 'none'."
      ;;
  esac
}

run_mysqlsh_installer() {
  local os_family="$1"
  local platform_dir
  local installer

  platform_dir="$(resolve_platform_dir "$os_family")" || return 1
  installer="$platform_dir/install_mysql_shell_innovation.sh"
  if [[ ! -x "$installer" ]]; then
    echo "Installer script not found or not executable: $installer" >&2
    return 1
  fi
  "$installer"
}

resolve_platform_dir() {
  local os_family="$1"
  local candidate
  local lowercase_dir="$SCRIPT_DIR/$os_family"
  local uppercase_dir="$SCRIPT_DIR/$(printf '%s' "$os_family" | tr '[:lower:]' '[:upper:]')"

  for candidate in "$lowercase_dir" "$uppercase_dir"; do
    if [[ -d "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "Platform directory not found for '$os_family'. Checked: $lowercase_dir and $uppercase_dir" >&2
  return 1
}

ensure_python() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required but was not found in PATH." >&2
    return 1
  fi
}

main() {
  local os_family="$OS_FAMILY_INPUT"
  local deploy_mode
  local host_value
  local http_port
  local https_port
  local ssl_cert_file
  local ssl_key_file
  local service_user=""
  local service_group=""
  local prompted_ports
  local tls_assets

  load_existing_runtime_env
  parse_args "$@"
  os_family="$OS_FAMILY_INPUT"

  ensure_python

  if [[ -z "$os_family" ]]; then
    os_family="$(detect_os_family)"
    if is_interactive_terminal; then
      os_family="$(prompt_for_normalized_value "OS family" "$os_family" normalize_os_family "Enter one of: ol8, ol9, ubuntu, macos.")"
    fi
  else
    os_family="$(normalize_os_family "$os_family")"
  fi

  if [[ -z "$DEPLOY_MODE_INPUT" ]]; then
    deploy_mode="http"
    if is_interactive_terminal; then
      deploy_mode="$(prompt_for_normalized_value "Deploy mode" "$deploy_mode" normalize_deploy_mode "Enter one of: http, https, both, none.")"
    fi
  else
    deploy_mode="$(normalize_deploy_mode "$DEPLOY_MODE_INPUT")"
  fi

  host_value="$(resolve_value "$HOST_INPUT" "$EXISTING_HOST" "0.0.0.0")"
  if is_interactive_terminal && [[ -z "$HOST_INPUT" ]]; then
    host_value="$(prompt_for_text_value "Host bind address" "$host_value" "no")"
  fi

  http_port="$(normalize_port "HTTP" "$(resolve_value "$HTTP_PORT_INPUT" "$EXISTING_DEFAULT_HTTP_PORT" "80")")"
  https_port="$(normalize_port "HTTPS" "$(resolve_value "$HTTPS_PORT_INPUT" "$EXISTING_DEFAULT_HTTPS_PORT" "443")")"
  prompted_ports="$(prompt_for_ports_if_needed "$deploy_mode" "$http_port" "$https_port")"
  http_port="$(printf '%s\n' "$prompted_ports" | sed -n '1p')"
  https_port="$(printf '%s\n' "$prompted_ports" | sed -n '2p')"

  ssl_cert_file="$(resolve_value "$SSL_CERT_FILE_INPUT" "$EXISTING_SSL_CERT_FILE" "")"
  ssl_key_file="$(resolve_value "$SSL_KEY_FILE_INPUT" "$EXISTING_SSL_KEY_FILE" "")"
  case "$deploy_mode" in
    https|both)
      if is_interactive_terminal && [[ -z "$SSL_CERT_FILE_INPUT" ]]; then
        ssl_cert_file="$(prompt_for_text_value "SSL certificate file" "$ssl_cert_file" "yes")"
      fi
      if is_interactive_terminal && [[ -z "$SSL_KEY_FILE_INPUT" ]]; then
        ssl_key_file="$(prompt_for_text_value "SSL private key file" "$ssl_key_file" "yes")"
      fi
      ;;
  esac

  case "$os_family" in
    ol8|ol9|ubuntu)
      service_user="$(resolve_service_user)"
      if is_interactive_terminal && [[ -z "$SERVICE_USER_INPUT" ]]; then
        service_user="$(prompt_for_text_value "Systemd service user" "$service_user" "no")"
      fi
      SERVICE_USER_INPUT="$service_user"

      service_group="$(resolve_service_group "$service_user")"
      if is_interactive_terminal && [[ -z "$SERVICE_GROUP_INPUT" ]]; then
        service_group="$(prompt_for_text_value "Systemd service group" "$service_group" "no")"
      fi
      SERVICE_GROUP_INPUT="$service_group"
      ;;
  esac

  tls_assets="$(ensure_https_tls_assets "$deploy_mode" "$host_value" "$ssl_cert_file" "$ssl_key_file" "$service_user" "$service_group")"
  ssl_cert_file="$(printf '%s\n' "$tls_assets" | sed -n '1p')"
  ssl_key_file="$(printf '%s\n' "$tls_assets" | sed -n '2p')"

  python3 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/python" -m pip install --upgrade pip wheel
  "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"

  run_mysqlsh_installer "$os_family"
  write_runtime_env "$http_port" "$https_port" "$host_value" "$ssl_cert_file" "$ssl_key_file"
  setup_systemd_services "$os_family" "$deploy_mode" "$ssl_cert_file" "$ssl_key_file"

  case "$deploy_mode" in
    http)
      open_firewall_port "HTTP" "$http_port"
      ;;
    https)
      open_firewall_port "HTTPS" "$https_port"
      ;;
    both)
      open_firewall_port "HTTP" "$http_port"
      open_firewall_port "HTTPS" "$https_port"
      ;;
    none)
      echo "Skipping firewall changes because deploy mode is 'none'."
      ;;
  esac

  echo "Setup completed."
  echo "Virtual environment: $VENV_DIR"
  echo "Saved runtime defaults: $RUNTIME_ENV_FILE"
  echo "Default host: $host_value"
  echo "Default HTTP port: $http_port"
  echo "Default HTTPS port: $https_port"
  if [[ -n "$ssl_cert_file" && -n "$ssl_key_file" ]]; then
    echo "TLS certificate: $ssl_cert_file"
    echo "TLS key: $ssl_key_file"
  fi
  echo "HTTP start script: $SCRIPT_DIR/start_http.sh"
  echo "HTTPS start script: $SCRIPT_DIR/start_https.sh"
  case "$os_family" in
    ol8|ol9|ubuntu)
      echo "Systemd services: ${APP_SLUG}-http.service and ${APP_SLUG}-https.service"
      ;;
  esac
  echo "Use PORT=<port> at launch time to override either saved default temporarily."
}

main "$@"
