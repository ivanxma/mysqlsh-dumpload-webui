#!/usr/bin/env bash

# When setup.sh is streamed into a shell there is no file-backed script path, so
# clone the repo first and then re-run the on-disk setup.sh with bash.
if [ -z "${BASH_VERSION:-}" ] || [ -z "${BASH_SOURCE:-}" ]; then
  set -eu

  bootstrap_print() {
    printf '%s\n' "$*" >&2
  }

  bootstrap_has_command() {
    command -v "$1" >/dev/null 2>&1
  }

  bootstrap_run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
      "$@"
    elif bootstrap_has_command sudo; then
      sudo "$@"
    else
      bootstrap_print "This step requires root privileges. Re-run as root or install sudo first."
      return 1
    fi
  }

  bootstrap_detect_os_family() {
    if [ "$(uname -s)" = "Darwin" ]; then
      printf '%s\n' "macos"
      return 0
    fi

    if [ ! -r /etc/os-release ]; then
      bootstrap_print "Unable to detect the operating system. Install git manually and rerun setup."
      return 1
    fi

    # shellcheck disable=SC1091
    . /etc/os-release
    case "$(printf '%s' "${ID:-unknown}" | tr '[:upper:]' '[:lower:]'):${VERSION_ID%%.*}" in
      ol:8|oraclelinux:8) printf '%s\n' "ol8" ;;
      ol:9|oraclelinux:9) printf '%s\n' "ol9" ;;
      ubuntu:*) printf '%s\n' "ubuntu" ;;
      *)
        bootstrap_print "Unsupported operating system: ${ID:-unknown} ${VERSION_ID:-unknown}. Install git manually and rerun setup."
        return 1
        ;;
    esac
  }

  bootstrap_install_git() {
    if bootstrap_has_command git; then
      return 0
    fi

    bootstrap_os_family="$(bootstrap_detect_os_family)" || return 1
    bootstrap_print "git was not found. Installing git for ${bootstrap_os_family}."

    case "$bootstrap_os_family" in
      ubuntu)
        bootstrap_run_as_root apt-get update
        bootstrap_run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y git
        ;;
      ol8|ol9)
        if bootstrap_has_command dnf; then
          bootstrap_run_as_root dnf install -y git
        elif bootstrap_has_command yum; then
          bootstrap_run_as_root yum install -y git
        else
          bootstrap_print "Neither dnf nor yum was found. Install git manually and rerun setup."
          return 1
        fi
        ;;
      macos)
        if bootstrap_has_command brew; then
          brew install git
        else
          if bootstrap_has_command xcode-select; then
            bootstrap_print "git was not found. Triggering Xcode Command Line Tools installation."
            xcode-select --install >/dev/null 2>&1 || true
          fi
          bootstrap_print "Install Xcode Command Line Tools or Homebrew, then rerun setup."
          return 1
        fi
        ;;
    esac

    if ! bootstrap_has_command git; then
      bootstrap_print "git installation did not complete successfully."
      return 1
    fi
  }

  bootstrap_timestamp() {
    date '+%Y%m%d%H%M%S'
  }

  bootstrap_prepare_target_dir() {
    if [ ! -e "$BOOTSTRAP_TARGET_DIR" ]; then
      return 0
    fi

    BOOTSTRAP_BACKUP_DIR="${BOOTSTRAP_TARGET_DIR}.$(bootstrap_timestamp)"
    while [ -e "$BOOTSTRAP_BACKUP_DIR" ]; do
      sleep 1
      BOOTSTRAP_BACKUP_DIR="${BOOTSTRAP_TARGET_DIR}.$(bootstrap_timestamp)"
    done

    bootstrap_print "Renaming existing $BOOTSTRAP_TARGET_DIR to $BOOTSTRAP_BACKUP_DIR"
    mv "$BOOTSTRAP_TARGET_DIR" "$BOOTSTRAP_BACKUP_DIR"
  }

  bootstrap_exec_cloned_setup() {
    if ! bootstrap_has_command bash; then
      bootstrap_print "bash is required to continue after cloning."
      return 1
    fi

    exec bash "$BOOTSTRAP_TARGET_DIR/setup.sh" "$@"
  }

  if [ -n "${0:-}" ] && [ -f "$0" ] && [ -r "$0" ]; then
    if ! bootstrap_has_command bash; then
      bootstrap_print "bash is required to run setup.sh."
      exit 1
    fi

    exec bash "$0" "$@"
  fi

  BOOTSTRAP_REPO_URL="${BOOTSTRAP_REPO_URL:-https://github.com/ivanxma/mysqlsh-dumpload-webui.git}"
  bootstrap_repo_name="${BOOTSTRAP_REPO_URL##*/}"
  bootstrap_repo_name="${bootstrap_repo_name%.git}"
  BOOTSTRAP_CLONE_DIR="${BOOTSTRAP_CLONE_DIR:-$bootstrap_repo_name}"
  BOOTSTRAP_PARENT_DIR="${BOOTSTRAP_PARENT_DIR:-$(pwd -P)}"
  BOOTSTRAP_TARGET_DIR="${BOOTSTRAP_PARENT_DIR%/}/$BOOTSTRAP_CLONE_DIR"

  bootstrap_install_git

  mkdir -p "$BOOTSTRAP_PARENT_DIR"
  cd "$BOOTSTRAP_PARENT_DIR"
  bootstrap_prepare_target_dir

  bootstrap_print "Cloning $BOOTSTRAP_REPO_URL into $BOOTSTRAP_TARGET_DIR"
  git clone "$BOOTSTRAP_REPO_URL" "$BOOTSTRAP_TARGET_DIR"

  if [ ! -r "$BOOTSTRAP_TARGET_DIR/setup.sh" ]; then
    bootstrap_print "The cloned repository does not contain setup.sh at $BOOTSTRAP_TARGET_DIR/setup.sh"
    exit 1
  fi

  bootstrap_exec_cloned_setup "$@"
fi

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
MYSQLSH_BINARY_INPUT="${MYSQLSH_BINARY:-}"
SKIP_PRIVILEGED_SETUP="${SKIP_PRIVILEGED_SETUP:-}"
MYSQLSH_EMBEDDED_VERSION="${MYSQLSH_EMBEDDED_VERSION:-9.7.0}"
MYSQLSH_URL_MACOS_X86="${MYSQLSH_URL_MACOS_X86:-https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-${MYSQLSH_EMBEDDED_VERSION}-macos15-x86-64bit.tar.gz}"
MYSQLSH_URL_MACOS_ARM="${MYSQLSH_URL_MACOS_ARM:-https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-${MYSQLSH_EMBEDDED_VERSION}-macos15-arm64.tar.gz}"
MYSQLSH_URL_LINUX_X86="${MYSQLSH_URL_LINUX_X86:-https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-${MYSQLSH_EMBEDDED_VERSION}-linux-glibc2.28-x86-64bit.tar.gz}"
MYSQLSH_URL_LINUX_ARM="${MYSQLSH_URL_LINUX_ARM:-https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-${MYSQLSH_EMBEDDED_VERSION}-linux-glibc2.28-arm-64bit.tar.gz}"
MYSQLSH_RUNTIME_DIR="${MYSQLSH_RUNTIME_DIR:-$SCRIPT_DIR/runtime/mysqlsh}"
MYSQLSH_DOWNLOADS_DIR="${MYSQLSH_DOWNLOADS_DIR:-$SCRIPT_DIR/runtime/downloads}"
MYSQL_SERVER_EMBEDDED_VERSION="${MYSQL_SERVER_EMBEDDED_VERSION:-9.7.0}"
MYSQL_SERVER_BRIDGE_VERSION="${MYSQL_SERVER_BRIDGE_VERSION:-8.4.8}"
MYSQL_SERVER_URL_LINUX_X86="${MYSQL_SERVER_URL_LINUX_X86:-https://dev.mysql.com/get/Downloads/MySQL-${MYSQL_SERVER_EMBEDDED_VERSION%.*}/mysql-${MYSQL_SERVER_EMBEDDED_VERSION}-linux-glibc2.28-x86_64.tar.xz}"
MYSQL_SERVER_URL_LINUX_ARM="${MYSQL_SERVER_URL_LINUX_ARM:-https://dev.mysql.com/get/Downloads/MySQL-${MYSQL_SERVER_EMBEDDED_VERSION%.*}/mysql-${MYSQL_SERVER_EMBEDDED_VERSION}-linux-glibc2.28-aarch64.tar.xz}"
MYSQL_SERVER_BRIDGE_URL_LINUX_X86="${MYSQL_SERVER_BRIDGE_URL_LINUX_X86:-https://dev.mysql.com/get/Downloads/MySQL-${MYSQL_SERVER_BRIDGE_VERSION%.*}/mysql-${MYSQL_SERVER_BRIDGE_VERSION}-linux-glibc2.28-x86_64.tar.xz}"
MYSQL_SERVER_BRIDGE_URL_LINUX_ARM="${MYSQL_SERVER_BRIDGE_URL_LINUX_ARM:-https://dev.mysql.com/get/Downloads/MySQL-${MYSQL_SERVER_BRIDGE_VERSION%.*}/mysql-${MYSQL_SERVER_BRIDGE_VERSION}-linux-glibc2.28-aarch64.tar.xz}"
MYSQL_SERVER_RUNTIME_DIR="${MYSQL_SERVER_RUNTIME_DIR:-$SCRIPT_DIR/.embedded/mysql-server}"
MYSQL_SERVER_DOWNLOADS_DIR="${MYSQL_SERVER_DOWNLOADS_DIR:-$SCRIPT_DIR/runtime/downloads}"
FLASK_SECRET_KEY_FILE="${FLASK_SECRET_KEY_FILE:-$SCRIPT_DIR/.flask_secret_key}"
MYSQL_SHELL_WEB_PYTHON_BIN="${MYSQL_SHELL_WEB_PYTHON_BIN:-}"
MYSQL_SHELL_WEB_PYTHON_MIN_VERSION="${MYSQL_SHELL_WEB_PYTHON_MIN_VERSION:-3.12}"
MYSQL_SHELL_WEB_DEPENDENCY_AUDIT="${MYSQL_SHELL_WEB_DEPENDENCY_AUDIT:-warn}"
MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT="${MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT:-0}"
MYSQL_SHELL_WEB_UPDATE_CODE_REFRESH_ONLY="${MYSQL_SHELL_WEB_UPDATE_CODE_REFRESH_ONLY:-0}"
MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES="${MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES:-9}"
LOCAL_MYSQL_PROFILE_NAME="${LOCAL_MYSQL_PROFILE_NAME:-local-admin-profile}"
LOCAL_MYSQL_ADMIN_USER="${LOCAL_MYSQL_ADMIN_USER:-localadmin}"
LOCAL_MYSQL_ADMIN_PASSWORD="${LOCAL_MYSQL_ADMIN_PASSWORD:-}"
LOCAL_MYSQL_SOCKET="${LOCAL_MYSQL_SOCKET:-$SCRIPT_DIR/.data/run/mysql.sock}"
LOCAL_MYSQL_DATABASE="${LOCAL_MYSQL_DATABASE:-mysql}"
LOCAL_MYSQL_CNF="$SCRIPT_DIR/etc/my.cnf"
LOCAL_MYSQL_DATADIR="$SCRIPT_DIR/.data/mysql"
LOCAL_MYSQL_RUN_DIR="$SCRIPT_DIR/.data/run"
LOCAL_MYSQL_LOG_DIR="$SCRIPT_DIR/.data/log"
LOCAL_MYSQL_TMP_DIR="$SCRIPT_DIR/.data/tmp"
LOCAL_MYSQL_BOOTSTRAP_DEFAULTS="$SCRIPT_DIR/.data/mysql-bootstrap.cnf"
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
  curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/setup.sh | sh -s -- [args]

Arguments:
  os_family    ol8 | ol9 | ubuntu | macos
  deploy_mode  http | https | both | none

Environment overrides:
  OS_FAMILY, DEPLOY_MODE, HOST, HTTP_PORT, HTTPS_PORT, SSL_CERT_FILE,
  SSL_KEY_FILE, SERVICE_USER, SERVICE_GROUP, VENV_DIR, RUNTIME_ENV_FILE,
  MYSQLSH_BINARY, MYSQLSH_EMBEDDED_VERSION, MYSQLSH_RUNTIME_DIR,
  MYSQLSH_DOWNLOADS_DIR, MYSQL_SERVER_EMBEDDED_VERSION,
  MYSQL_SERVER_BRIDGE_VERSION, MYSQL_SERVER_URL_LINUX_X86,
  MYSQL_SERVER_URL_LINUX_ARM, MYSQL_SERVER_BRIDGE_URL_LINUX_X86,
  MYSQL_SERVER_BRIDGE_URL_LINUX_ARM, MYSQL_SERVER_RUNTIME_DIR,
  MYSQL_SERVER_DOWNLOADS_DIR,
  SKIP_PRIVILEGED_SETUP,
  MYSQL_SHELL_WEB_PYTHON_BIN, MYSQL_SHELL_WEB_PYTHON_MIN_VERSION,
  MYSQL_SHELL_WEB_DEPENDENCY_AUDIT, MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT,
  MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES,
  LOCAL_MYSQL_PROFILE_NAME, LOCAL_MYSQL_ADMIN_USER, LOCAL_MYSQL_ADMIN_PASSWORD,
  LOCAL_MYSQL_SOCKET, LOCAL_MYSQL_DATABASE

Bootstrap overrides for curl | sh:
  BOOTSTRAP_REPO_URL, BOOTSTRAP_CLONE_DIR, BOOTSTRAP_PARENT_DIR
EOF
}

is_interactive_terminal() {
  [[ -t 0 && -t 1 ]]
}

privileged_setup_skipped() {
  case "$(to_lower "$SKIP_PRIVILEGED_SETUP")" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
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

run_as_root() {
  if [[ $EUID -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    echo "Root privileges are required for: $*" >&2
    return 1
  fi
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

  unset DEFAULT_HTTP_PORT DEFAULT_HTTPS_PORT HOST SSL_CERT_FILE SSL_KEY_FILE MYSQLSH_BINARY
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
  local require_explicit_value="${3:-no}"
  local entered_value
  local normalized_value

  while true; do
    if [[ "$require_explicit_value" == "yes" ]]; then
      printf '%s port [%s] (required): ' "$label" "$current_value" >&2
    else
      printf '%s port [%s]: ' "$label" "$current_value" >&2
    fi
    if ! read -r entered_value; then
      echo >&2
      echo "$current_value"
      return 0
    fi
    if [[ -z "$entered_value" ]]; then
      if [[ "$require_explicit_value" == "yes" ]]; then
        echo "${label} port is required. Enter a numeric port between 1 and 65535." >&2
        continue
      fi
      echo "$current_value"
      return 0
    fi

    if normalized_value="$(normalize_port "$label" "$entered_value" 2>/dev/null)"; then
      echo "$normalized_value"
      return 0
    fi

    if [[ "$require_explicit_value" == "yes" ]]; then
      echo "Enter a numeric port between 1 and 65535." >&2
    else
      echo "Enter a numeric port between 1 and 65535, or press Enter to keep $current_value." >&2
    fi
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
        http_port="$(prompt_for_port_value "HTTP" "$http_port" "yes")"
      fi
      ;;
    https)
      if [[ -z "$HTTPS_PORT_INPUT" ]]; then
        https_port="$(prompt_for_port_value "HTTPS" "$https_port" "yes")"
      fi
      ;;
    both)
      if [[ -z "$HTTP_PORT_INPUT" ]]; then
        http_port="$(prompt_for_port_value "HTTP" "$http_port" "yes")"
      fi
      if [[ -z "$HTTPS_PORT_INPUT" ]]; then
        https_port="$(prompt_for_port_value "HTTPS" "$https_port" "yes")"
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

resolve_machine_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "x86_64" ;;
    arm64|arm64e|aarch64) echo "arm64" ;;
    *)
      echo "Unsupported machine architecture '$(uname -m)'. Supported: x86_64, arm64." >&2
      return 1
      ;;
  esac
}

resolve_mysqlsh_download_url() {
  local os_family="$1"
  local machine_arch

  machine_arch="$(resolve_machine_arch)" || return 1
  case "${os_family}:${machine_arch}" in
    macos:x86_64) echo "$MYSQLSH_URL_MACOS_X86" ;;
    macos:arm64) echo "$MYSQLSH_URL_MACOS_ARM" ;;
    ubuntu:x86_64|ol8:x86_64|ol9:x86_64) echo "$MYSQLSH_URL_LINUX_X86" ;;
    ubuntu:arm64|ol8:arm64|ol9:arm64) echo "$MYSQLSH_URL_LINUX_ARM" ;;
    *)
      echo "No embedded MySQL Shell tarball is configured for OS family '$os_family' on architecture '$machine_arch'." >&2
      return 1
      ;;
  esac
}

resolve_mysql_server_download_url() {
  local os_family="$1"
  local version_kind="${2:-target}"
  local machine_arch

  machine_arch="$(resolve_machine_arch)" || return 1
  case "${os_family}:${machine_arch}:${version_kind}" in
    ubuntu:x86_64:target|ol8:x86_64:target|ol9:x86_64:target) echo "$MYSQL_SERVER_URL_LINUX_X86" ;;
    ubuntu:arm64:target|ol8:arm64:target|ol9:arm64:target) echo "$MYSQL_SERVER_URL_LINUX_ARM" ;;
    ubuntu:x86_64:bridge|ol8:x86_64:bridge|ol9:x86_64:bridge) echo "$MYSQL_SERVER_BRIDGE_URL_LINUX_X86" ;;
    ubuntu:arm64:bridge|ol8:arm64:bridge|ol9:arm64:bridge) echo "$MYSQL_SERVER_BRIDGE_URL_LINUX_ARM" ;;
    *)
      echo "No embedded MySQL Server tarball is configured for OS family '$os_family' on architecture '$machine_arch'." >&2
      return 1
      ;;
  esac
}

require_download_tool() {
  if command -v curl >/dev/null 2>&1; then
    echo "curl"
    return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    echo "wget"
    return 0
  fi

  echo "curl or wget is required to download the embedded MySQL Shell tarball." >&2
  return 1
}

download_file() {
  local url="$1"
  local destination="$2"
  local downloader
  local temp_destination="${destination}.part"

  downloader="$(require_download_tool)" || return 1
  rm -f "$temp_destination"

  case "$downloader" in
    curl)
      curl --fail --location --show-error --retry 3 --output "$temp_destination" "$url"
      ;;
    wget)
      wget --tries=3 --output-document "$temp_destination" "$url"
      ;;
  esac

  mv "$temp_destination" "$destination"
}

strip_tarball_suffix() {
  local filename="$1"

  filename="${filename%.tar.xz}"
  filename="${filename%.tar.gz}"
  filename="${filename%.tgz}"
  printf '%s\n' "$filename"
}

extract_tarball_root() {
  local archive_path="$1"
  local archive_root

  case "$archive_path" in
    *.tar.xz) archive_root="$(tar -tJf "$archive_path" | sed -n '1s#/.*##p')" ;;
    *) archive_root="$(tar -tzf "$archive_path" | sed -n '1s#/.*##p')" ;;
  esac
  if [[ -z "$archive_root" ]]; then
    echo "Unable to determine the extracted root directory for $archive_path." >&2
    return 1
  fi

  printf '%s\n' "$archive_root"
}

extract_tarball() {
  local archive_path="$1"
  local target_dir="$2"
  case "$archive_path" in
    *.tar.xz) tar -xJf "$archive_path" -C "$target_dir" ;;
    *) tar -xzf "$archive_path" -C "$target_dir" ;;
  esac
}

install_embedded_mysqlsh() {
  local os_family="$1"
  local download_url
  local archive_name
  local archive_path
  local target_dir
  local current_link
  local staging_dir=""
  local archive_root

  download_url="$(resolve_mysqlsh_download_url "$os_family")" || return 1
  archive_name="${download_url##*/}"
  archive_path="$MYSQLSH_DOWNLOADS_DIR/$archive_name"
  target_dir="$MYSQLSH_RUNTIME_DIR/$(strip_tarball_suffix "$archive_name")"
  current_link="$MYSQLSH_RUNTIME_DIR/current"

  mkdir -p "$MYSQLSH_RUNTIME_DIR" "$MYSQLSH_DOWNLOADS_DIR"

  if [[ ! -x "$target_dir/bin/mysqlsh" ]]; then
    if [[ ! -f "$archive_path" ]]; then
      echo "Downloading embedded MySQL Shell ${MYSQLSH_EMBEDDED_VERSION}: $archive_name" >&2
      download_file "$download_url" "$archive_path"
    else
      echo "Reusing downloaded MySQL Shell archive: $archive_path" >&2
    fi

    archive_root="$(extract_tarball_root "$archive_path")" || return 1
    staging_dir="$(mktemp -d "$MYSQLSH_RUNTIME_DIR/.extract.XXXXXX")"
    extract_tarball "$archive_path" "$staging_dir"

    if [[ ! -d "$staging_dir/$archive_root" ]]; then
      echo "Expected extracted directory '$archive_root' was not found in $archive_path." >&2
      rm -rf "$staging_dir"
      return 1
    fi

    rm -rf "$target_dir"
    mv "$staging_dir/$archive_root" "$target_dir"
    rm -rf "$staging_dir"
  else
    echo "Reusing embedded MySQL Shell: $target_dir" >&2
  fi

  ln -sfn "$target_dir" "$current_link"
  if [[ ! -x "$current_link/bin/mysqlsh" ]]; then
    echo "Embedded MySQL Shell binary was not found at $current_link/bin/mysqlsh." >&2
    return 1
  fi

  printf '%s\n' "$current_link/bin/mysqlsh"
}

run_mysqlsh_installer() {
  local os_family="$1"
  local configured_binary="$MYSQLSH_BINARY_INPUT"
  local installer_script="$SCRIPT_DIR/$os_family/install_mysql_shell_innovation.sh"
  local resolved_mysqlsh

  if [[ -n "$configured_binary" ]]; then
    if [[ ! -x "$configured_binary" ]]; then
      echo "MYSQLSH_BINARY points to a non-executable path: $configured_binary" >&2
      return 1
    fi
    printf '%s\n' "$configured_binary"
    return 0
  fi

  if [[ -x "$installer_script" && ! privileged_setup_skipped ]]; then
    if "$installer_script" >&2; then
      resolved_mysqlsh="$(command -v mysqlsh || true)"
      if [[ -n "$resolved_mysqlsh" && -x "$resolved_mysqlsh" ]]; then
        printf '%s\n' "$resolved_mysqlsh"
        return 0
      fi
      echo "The platform MySQL Shell installer completed but mysqlsh was not found in PATH. Falling back to embedded MySQL Shell." >&2
    else
      echo "The platform MySQL Shell installer did not complete. Falling back to embedded MySQL Shell." >&2
    fi
  elif privileged_setup_skipped; then
    echo "Skipping platform MySQL Shell installer because SKIP_PRIVILEGED_SETUP is set. Using embedded MySQL Shell." >&2
  fi

  install_embedded_mysqlsh "$os_family"
}

write_runtime_env() {
  local http_port="$1"
  local https_port="$2"
  local host_value="$3"
  local ssl_cert_file="$4"
  local ssl_key_file="$5"
  local mysqlsh_binary="$6"
  local os_family="$7"
  local deploy_mode="$8"

  {
    echo "# Generated by setup.sh"
    echo "OS_FAMILY=$os_family"
    echo "DEPLOY_MODE=$deploy_mode"
    echo "HOST=$host_value"
    echo "DEFAULT_HTTP_PORT=$http_port"
    echo "DEFAULT_HTTPS_PORT=$https_port"
    echo "MYSQLSH_BINARY=$mysqlsh_binary"
    echo "FLASK_SECRET_KEY_FILE=$FLASK_SECRET_KEY_FILE"
    echo "MYSQL_SHELL_WEB_PYTHON_BIN=${MYSQL_SHELL_WEB_PYTHON_BIN:-}"
    echo "MYSQL_SHELL_WEB_PYTHON_MIN_VERSION=$MYSQL_SHELL_WEB_PYTHON_MIN_VERSION"
    echo "MYSQL_SHELL_WEB_DEPENDENCY_AUDIT=$MYSQL_SHELL_WEB_DEPENDENCY_AUDIT"
    echo "MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT=$MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT"
    echo "MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES=$MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES"
    echo "MYSQL_SERVER_EMBEDDED_VERSION=$MYSQL_SERVER_EMBEDDED_VERSION"
    echo "MYSQL_SERVER_BRIDGE_VERSION=$MYSQL_SERVER_BRIDGE_VERSION"
    echo "MYSQL_SERVER_RUNTIME_DIR=$MYSQL_SERVER_RUNTIME_DIR"
    echo "MYSQL_SERVER_DOWNLOADS_DIR=$MYSQL_SERVER_DOWNLOADS_DIR"
    echo "MYSQL_SERVER_URL_LINUX_X86=$MYSQL_SERVER_URL_LINUX_X86"
    echo "MYSQL_SERVER_URL_LINUX_ARM=$MYSQL_SERVER_URL_LINUX_ARM"
    echo "MYSQL_SERVER_BRIDGE_URL_LINUX_X86=$MYSQL_SERVER_BRIDGE_URL_LINUX_X86"
    echo "MYSQL_SERVER_BRIDGE_URL_LINUX_ARM=$MYSQL_SERVER_BRIDGE_URL_LINUX_ARM"
    if [[ "$deploy_mode" == "https" || "$deploy_mode" == "both" ]]; then
      echo "MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE=1"
    else
      echo "MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE=${MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE:-0}"
    fi
    echo "MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL=${MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL:-}"
    echo "MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH=${MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH:-}"
    echo "MYSQL_SHELL_WEB_VERSION_URL=${MYSQL_SHELL_WEB_VERSION_URL:-}"
    echo "MYSQL_SHELL_WEB_VERSION_CA_BUNDLE=${MYSQL_SHELL_WEB_VERSION_CA_BUNDLE:-}"
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

  if privileged_setup_skipped; then
    chmod 644 "$ssl_cert_file" 2>/dev/null || echo "Skipping TLS certificate permission update because SKIP_PRIVILEGED_SETUP is set." >&2
    chmod 600 "$ssl_key_file" 2>/dev/null || echo "Skipping TLS key permission update because SKIP_PRIVILEGED_SETUP is set." >&2
    echo "Skipping TLS ownership update because SKIP_PRIVILEGED_SETUP is set." >&2
    return 0
  fi

  chmod 644 "$ssl_cert_file"
  chmod 600 "$ssl_key_file"

  if [[ -n "$service_user" && -n "$service_group" ]]; then
    sudo chown "$service_user:$service_group" "$ssl_cert_file" "$ssl_key_file"
  fi
}

ensure_flask_secret_key() {
  if [[ -s "$FLASK_SECRET_KEY_FILE" ]]; then
    chmod 600 "$FLASK_SECRET_KEY_FILE" 2>/dev/null || true
    return 0
  fi

  mkdir -p "$(dirname "$FLASK_SECRET_KEY_FILE")"
  "$VENV_DIR/bin/python" - <<'PY' >"$FLASK_SECRET_KEY_FILE"
import secrets
print(secrets.token_hex(32))
PY
  chmod 600 "$FLASK_SECRET_KEY_FILE"
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
  local bind_capability="$6"
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
$(if [[ "$bind_capability" == "yes" ]]; then printf '%s\n' 'AmbientCapabilities=CAP_NET_BIND_SERVICE'; fi)
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
  local http_port="${5:-}"
  local https_port="${6:-}"
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

  if privileged_setup_skipped; then
    echo "Skipping systemd service setup because SKIP_PRIVILEGED_SETUP is set." >&2
    return 0
  fi

  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl was not found. Create the service manually if you need background startup on this host." >&2
    return 0
  fi

  service_user="$(resolve_service_user)"
  service_group="$(resolve_service_group "$service_user")"

  install_systemd_service "$http_service" "$APP_NAME HTTP service" "$SCRIPT_DIR/start_http.sh" "$service_user" "$service_group" "$(if [[ "$http_port" =~ ^[0-9]+$ ]] && (( http_port < 1024 )); then echo yes; else echo no; fi)"
  install_systemd_service "$https_service" "$APP_NAME HTTPS service" "$SCRIPT_DIR/start_https.sh" "$service_user" "$service_group" "$(if [[ "$https_port" =~ ^[0-9]+$ ]] && (( https_port < 1024 )); then echo yes; else echo no; fi)"
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

setup_local_mysql_systemd_service() {
  local os_family="$1"
  local service_user
  local service_group
  local service_name="${APP_SLUG}-local-mysql"
  local unit_path="/etc/systemd/system/${service_name}.service"
  local mysqld_bin
  local bash_bin

  case "$os_family" in
    ol8|ol9|ubuntu) ;;
    *)
      return 0
      ;;
  esac

  if privileged_setup_skipped; then
    echo "Skipping local MySQL systemd service setup because SKIP_PRIVILEGED_SETUP is set." >&2
    return 0
  fi

  if [[ ! -d "$LOCAL_MYSQL_DATADIR/mysql" || ! -f "$LOCAL_MYSQL_CNF" ]]; then
    return 0
  fi

  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl was not found. Start local MySQL manually before using local-admin-profile." >&2
    return 0
  fi

  service_user="$(resolve_service_user)"
  service_group="$(resolve_service_group "$service_user")"
  mysqld_bin="$(local_mysql_mysqld_bin)"
  bash_bin="$(resolve_bash_bin)" || return 1

  sudo tee "$unit_path" >/dev/null <<EOF
[Unit]
Description=$APP_NAME local embedded MySQL service
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=$service_user
Group=$service_group
WorkingDirectory=$SCRIPT_DIR
PIDFile=$LOCAL_MYSQL_RUN_DIR/mysqld.pid
ExecStart=$bash_bin -lc 'exec "$mysqld_bin" --defaults-file="$LOCAL_MYSQL_CNF" --daemonize'
Restart=on-failure
RestartSec=5
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
EOF

  stop_local_mysql
  sudo systemctl daemon-reload
  sudo systemctl enable --now "${service_name}.service"
  echo "Enabled systemd service ${service_name}.service."
}

repair_local_permissions() {
  local owner_user="${SERVICE_USER_INPUT:-}"
  local owner_group="${SERVICE_GROUP_INPUT:-$owner_user}"
  if [[ -n "$owner_user" && -n "$owner_group" ]] && ! privileged_setup_skipped; then
    for path in \
      "$RUNTIME_ENV_FILE" \
      "$FLASK_SECRET_KEY_FILE" \
      "$SCRIPT_DIR/profiles.json" \
      "$SCRIPT_DIR/object_storage.json" \
      "$SCRIPT_DIR/mysqlsh_option_profiles.json" \
      "$SCRIPT_DIR/etc" \
      "$SCRIPT_DIR/.data" \
      "$SCRIPT_DIR/.embedded" \
      "$SCRIPT_DIR/runtime" \
      "$SCRIPT_DIR/profile_ssh_keys" \
      "$SCRIPT_DIR/tls"; do
      if [[ -e "$path" ]]; then
        run_as_root chown -R "$owner_user:$owner_group" "$path" 2>/dev/null || true
      fi
    done
  fi
  chmod 600 "$RUNTIME_ENV_FILE" 2>/dev/null || true
  chmod 600 "$FLASK_SECRET_KEY_FILE" 2>/dev/null || true
  chmod 600 "$SCRIPT_DIR/profiles.json" "$SCRIPT_DIR/object_storage.json" 2>/dev/null || true
  chmod 600 "$SCRIPT_DIR"/runtime/updates/* 2>/dev/null || true
  chmod 700 "$SCRIPT_DIR/profile_ssh_keys" "$SCRIPT_DIR/tls" "$SCRIPT_DIR/.data" "$SCRIPT_DIR/.embedded" 2>/dev/null || true
  find "$SCRIPT_DIR/profile_ssh_keys" -type d -exec chmod 700 {} \; 2>/dev/null || true
  find "$SCRIPT_DIR/profile_ssh_keys" -type f -exec chmod 600 {} \; 2>/dev/null || true
  find "$SCRIPT_DIR/tls" -type f \( -name '*.key' -o -name '*.pem' -o -name '*.p12' -o -name '*.pfx' -o -name '*.jks' -o -name '*.keystore' \) -exec chmod 600 {} \; 2>/dev/null || true
}

install_embedded_mysql_server() {
  local os_family="$1"
  local version_kind="${2:-target}"
  local download_url
  local archive_name
  local archive_path
  local target_dir
  local current_link
  local staging_dir=""
  local archive_root
  local display_version="$MYSQL_SERVER_EMBEDDED_VERSION"

  if [[ "$version_kind" == "bridge" ]]; then
    display_version="$MYSQL_SERVER_BRIDGE_VERSION"
  fi

  download_url="$(resolve_mysql_server_download_url "$os_family" "$version_kind")" || return 1
  archive_name="${download_url##*/}"
  archive_path="$MYSQL_SERVER_DOWNLOADS_DIR/$archive_name"
  target_dir="$MYSQL_SERVER_RUNTIME_DIR/$(strip_tarball_suffix "$archive_name")"
  current_link="$MYSQL_SERVER_RUNTIME_DIR/current"

  mkdir -p "$MYSQL_SERVER_RUNTIME_DIR" "$MYSQL_SERVER_DOWNLOADS_DIR"

  if [[ ! -x "$target_dir/bin/mysqld" || ! -x "$target_dir/bin/mysql" ]]; then
    if [[ ! -f "$archive_path" ]]; then
      echo "Downloading embedded MySQL Server ${display_version}: $archive_name" >&2
      download_file "$download_url" "$archive_path"
    else
      echo "Reusing downloaded MySQL Server archive: $archive_path" >&2
    fi

    archive_root="$(extract_tarball_root "$archive_path")" || return 1
    staging_dir="$(mktemp -d "$MYSQL_SERVER_RUNTIME_DIR/.extract.XXXXXX")"
    extract_tarball "$archive_path" "$staging_dir"

    if [[ ! -d "$staging_dir/$archive_root" ]]; then
      echo "Expected extracted directory '$archive_root' was not found in $archive_path." >&2
      rm -rf "$staging_dir"
      return 1
    fi

    rm -rf "$target_dir"
    mv "$staging_dir/$archive_root" "$target_dir"
    rm -rf "$staging_dir"
  else
    echo "Reusing embedded MySQL Server: $target_dir" >&2
  fi

  ln -sfn "$target_dir" "$current_link"
  if [[ ! -x "$current_link/bin/mysqld" || ! -x "$current_link/bin/mysql" ]]; then
    echo "Embedded MySQL Server binaries were not found under $current_link/bin." >&2
    return 1
  fi

  printf '%s\n' "$current_link"
}

mysql_server_version() {
  local basedir="${1:-}"
  local version_output
  local mysqld_bin="mysqld"
  if [[ -n "$basedir" ]]; then
    mysqld_bin="$basedir/bin/mysqld"
  fi
  version_output="$("$mysqld_bin" --version 2>/dev/null || true)"
  printf '%s\n' "$version_output" | sed -nE 's/.*Ver[[:space:]]+([0-9]+(\.[0-9]+){1,2}).*/\1/p' | head -n 1
}

mysql_server_matches_required_series() {
  local basedir="${1:-}"
  local version
  version="$(mysql_server_version "$basedir")"
  [[ -n "$version" && "$version" == "$MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES".* ]]
}

install_embedded_mysql_server_dependencies() {
  local os_family="$1"
  if privileged_setup_skipped; then
    return 0
  fi
  case "$os_family" in
    ol8|ol9)
      run_as_root dnf install -y libaio xz || true
      run_as_root dnf install -y ncurses-compat-libs || true
      ;;
    ubuntu)
      run_as_root apt-get update
      run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libaio1 libncurses6 xz-utils || \
        run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libaio-dev libncurses6 xz-utils || true
      ;;
  esac
}

install_mysql_server_binaries() {
  local os_family="$1"
  local basedir
  install_embedded_mysql_server_dependencies "$os_family" >&2
  basedir="$(install_embedded_mysql_server "$os_family" target)" || return 1
  if mysql_server_matches_required_series "$basedir"; then
    printf '%s\n' "$basedir"
    return 0
  fi
  echo "Embedded MySQL Server $(mysql_server_version "$basedir") is installed, but ${MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES}.x is required." >&2
  return 1
}

bridge_local_mysql_through_lts_if_supported() {
  local os_family="$1"
  local basedir
  echo "Attempting embedded MySQL ${MYSQL_SERVER_BRIDGE_VERSION} bridge upgrade before MySQL ${MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES}.x startup."
  basedir="$(install_embedded_mysql_server "$os_family" bridge)" || return 1
  write_local_mysql_config "$basedir"
  stop_local_mysql
  start_local_mysql
  stop_local_mysql
  install_mysql_server_binaries "$os_family"
}

write_local_mysql_config() {
  local basedir="$1"
  mkdir -p "$SCRIPT_DIR/etc" "$LOCAL_MYSQL_RUN_DIR" "$LOCAL_MYSQL_LOG_DIR" "$LOCAL_MYSQL_TMP_DIR"
  chmod 700 "$SCRIPT_DIR/.data" "$LOCAL_MYSQL_RUN_DIR" "$LOCAL_MYSQL_LOG_DIR" "$LOCAL_MYSQL_TMP_DIR" 2>/dev/null || true
  cat >"$LOCAL_MYSQL_CNF" <<EOF
[mysqld]
basedir=$basedir
datadir=$LOCAL_MYSQL_DATADIR
socket=$LOCAL_MYSQL_SOCKET
pid-file=$LOCAL_MYSQL_RUN_DIR/mysqld.pid
log-error=$LOCAL_MYSQL_LOG_DIR/mysqld.err
tmpdir=$LOCAL_MYSQL_TMP_DIR
skip-networking
mysqlx=0
EOF
  chmod 600 "$LOCAL_MYSQL_CNF"
}

local_mysql_basedir() {
  if [[ -r "$LOCAL_MYSQL_CNF" ]]; then
    sed -n 's/^basedir=//p' "$LOCAL_MYSQL_CNF" | tail -n 1
    return 0
  fi
  printf '%s\n' "$MYSQL_SERVER_RUNTIME_DIR/current"
}

local_mysql_mysqld_bin() {
  local basedir
  basedir="$(local_mysql_basedir)"
  printf '%s\n' "$basedir/bin/mysqld"
}

local_mysql_client_bin() {
  local basedir
  basedir="$(local_mysql_basedir)"
  printf '%s\n' "$basedir/bin/mysql"
}

wait_for_local_mysql_socket() {
  local i
  for i in $(seq 1 30); do
    if [[ -S "$LOCAL_MYSQL_SOCKET" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for local MySQL socket $LOCAL_MYSQL_SOCKET." >&2
  return 1
}

process_exists() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null || run_as_root kill -0 "$pid" 2>/dev/null
}

stop_local_mysql() {
  local pid
  if [[ ! -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" ]]; then
    return 0
  fi
  pid="$(cat "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" 2>/dev/null || true)"
  if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
    rm -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" "$LOCAL_MYSQL_SOCKET"
    return 0
  fi
  if ! process_exists "$pid"; then
    rm -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" "$LOCAL_MYSQL_SOCKET"
    return 0
  fi
  kill "$pid" 2>/dev/null || run_as_root kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 30); do
    if ! process_exists "$pid"; then
      rm -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" "$LOCAL_MYSQL_SOCKET"
      return 0
    fi
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || run_as_root kill -9 "$pid" 2>/dev/null || true
  rm -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" "$LOCAL_MYSQL_SOCKET"
}

start_local_mysql() {
  local mysqld_bin
  if [[ -f "$LOCAL_MYSQL_RUN_DIR/mysqld.pid" ]] && process_exists "$(cat "$LOCAL_MYSQL_RUN_DIR/mysqld.pid")"; then
    return 0
  fi
  mysqld_bin="$(local_mysql_mysqld_bin)"
  "$mysqld_bin" --defaults-file="$LOCAL_MYSQL_CNF" --daemonize
  wait_for_local_mysql_socket
}

extract_temporary_mysql_password() {
  sed -n "s/.*temporary password.*root@localhost: //p" "$LOCAL_MYSQL_LOG_DIR/mysqld.err" | tail -n 1
}

sql_quote() {
  printf "%s" "$1" | sed "s/'/''/g"
}

initialize_local_mysql_if_needed() {
  local os_family="$1"
  local basedir
  local temp_password
  local defaults_file
  local escaped_user
  local escaped_password
  local mysql_client

  basedir="$(install_mysql_server_binaries "$os_family")" || return 1
  write_local_mysql_config "$basedir"

  if [[ -d "$LOCAL_MYSQL_DATADIR/mysql" ]]; then
    stop_local_mysql
    if ! start_local_mysql; then
      bridge_local_mysql_through_lts_if_supported "$os_family"
      basedir="$(local_mysql_basedir)" || return 1
      write_local_mysql_config "$basedir"
      stop_local_mysql
      start_local_mysql
    fi
    return 0
  fi

  rm -rf "$LOCAL_MYSQL_DATADIR"
  "$(local_mysql_mysqld_bin)" --defaults-file="$LOCAL_MYSQL_CNF" --initialize
  temp_password="$(extract_temporary_mysql_password)"
  if [[ -z "$temp_password" ]]; then
    echo "Unable to read the temporary MySQL root password from $LOCAL_MYSQL_LOG_DIR/mysqld.err." >&2
    return 1
  fi

  start_local_mysql
  defaults_file="$LOCAL_MYSQL_BOOTSTRAP_DEFAULTS"
  cat >"$defaults_file" <<EOF
[client]
user=root
password=$temp_password
socket=$LOCAL_MYSQL_SOCKET
EOF
  chmod 600 "$defaults_file"
  escaped_user="$(sql_quote "$LOCAL_MYSQL_ADMIN_USER")"
  escaped_password="$(sql_quote "$LOCAL_MYSQL_ADMIN_PASSWORD")"
  mysql_client="$(local_mysql_client_bin)"
  "$mysql_client" --defaults-extra-file="$defaults_file" --connect-expired-password <<SQL
ALTER USER 'root'@'localhost' IDENTIFIED BY '${escaped_password}';
RENAME USER 'root'@'localhost' TO '${escaped_user}'@'localhost';
GRANT ALL PRIVILEGES ON *.* TO '${escaped_user}'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL
  rm -f "$defaults_file"
}

ensure_local_mysql_profile() {
  local os_family="$1"
  local profile_script="$SCRIPT_DIR/secured_connection_profile_setup.sh"
  local socket_path="$LOCAL_MYSQL_SOCKET"

  case "$(to_lower "$MYSQL_SHELL_WEB_UPDATE_CODE_REFRESH_ONLY")" in
    1|true|yes|on)
      echo "Skipping local-admin-profile bootstrap for old-version code-refresh compatibility. Rerun Auto-Update from the refreshed page with a temporary local admin password."
      return 0
      ;;
  esac

  if [[ ! -x "$profile_script" ]]; then
    chmod +x "$profile_script" 2>/dev/null || true
  fi

  if [[ -z "$LOCAL_MYSQL_ADMIN_PASSWORD" ]]; then
    if ! "$VENV_DIR/bin/python" - "$SCRIPT_DIR/profiles.json" "$LOCAL_MYSQL_PROFILE_NAME" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
profile_name = sys.argv[2]
if not path.exists():
    raise SystemExit(1)
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except json.JSONDecodeError:
    raise SystemExit(1)
for row in payload.get("profiles", []):
    if row.get("name") == profile_name and row.get("mode") == "socket" and row.get("socket"):
        raise SystemExit(0)
raise SystemExit(1)
PY
    then
      echo "local-admin-profile is missing or not socket-only. Set LOCAL_MYSQL_ADMIN_PASSWORD and rerun setup.sh, or use the refreshed Auto-Update page bootstrap fields." >&2
      return 0
    fi
    if [[ -d "$LOCAL_MYSQL_DATADIR/mysql" ]]; then
      initialize_local_mysql_if_needed "$os_family"
    fi
  fi

  if [[ ! -f "$SCRIPT_DIR/profiles.json" && -z "$LOCAL_MYSQL_ADMIN_PASSWORD" ]]; then
    echo "local-admin-profile is not configured. Set LOCAL_MYSQL_ADMIN_PASSWORD and rerun setup.sh to provision it." >&2
    return 0
  fi

  if [[ -n "$LOCAL_MYSQL_ADMIN_PASSWORD" ]]; then
    initialize_local_mysql_if_needed "$os_family"
  fi

  local profile_args=(
    --profile-store "$SCRIPT_DIR/profiles.json" \
    --ssh-key-dir "$SCRIPT_DIR/profile_ssh_keys" \
    --profile-name "$LOCAL_MYSQL_PROFILE_NAME" \
    --socket "$socket_path" \
    --admin-user "$LOCAL_MYSQL_ADMIN_USER" \
    --database "$LOCAL_MYSQL_DATABASE"
  )
  if [[ -z "$LOCAL_MYSQL_ADMIN_PASSWORD" ]]; then
    profile_args+=(--no-force-password-change)
  fi

  PYTHON_BIN="$VENV_DIR/bin/python" "$profile_script" "${profile_args[@]}"
}

python_version_at_least() {
  local python_bin="$1"
  local min_version="$2"
  "$python_bin" - "$min_version" <<'PY'
import sys
want = tuple(int(part) for part in sys.argv[1].split(".")[:2])
have = sys.version_info[:2]
raise SystemExit(0 if have >= want else 1)
PY
}

install_python_if_possible() {
  local os_family="$1"
  if privileged_setup_skipped; then
    echo "Python $MYSQL_SHELL_WEB_PYTHON_MIN_VERSION+ is missing and SKIP_PRIVILEGED_SETUP is set. Install it manually." >&2
    return 1
  fi
  case "$os_family" in
    ol8|ol9)
      sudo dnf install -y python3.12 python3.12-pip python3.12-devel >&2 || true
      ;;
    ubuntu)
      sudo apt-get update >&2
      sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12 python3.12-venv python3.12-dev >&2 || true
      ;;
    macos)
      if command -v brew >/dev/null 2>&1; then
        brew install python@3.12 >&2 || true
      fi
      ;;
  esac
}

select_python_bin() {
  local os_family="$1"
  local candidate
  if [[ -n "$MYSQL_SHELL_WEB_PYTHON_BIN" ]]; then
    if [[ -x "$MYSQL_SHELL_WEB_PYTHON_BIN" ]] && python_version_at_least "$MYSQL_SHELL_WEB_PYTHON_BIN" "$MYSQL_SHELL_WEB_PYTHON_MIN_VERSION"; then
      printf '%s\n' "$MYSQL_SHELL_WEB_PYTHON_BIN"
      return 0
    fi
    echo "MYSQL_SHELL_WEB_PYTHON_BIN does not satisfy Python >= $MYSQL_SHELL_WEB_PYTHON_MIN_VERSION: $MYSQL_SHELL_WEB_PYTHON_BIN" >&2
    return 1
  fi
  for candidate in python3.13 python3.12 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      candidate="$(command -v "$candidate")"
      if python_version_at_least "$candidate" "$MYSQL_SHELL_WEB_PYTHON_MIN_VERSION"; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi
  done
  install_python_if_possible "$os_family"
  for candidate in python3.13 python3.12 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      candidate="$(command -v "$candidate")"
      if python_version_at_least "$candidate" "$MYSQL_SHELL_WEB_PYTHON_MIN_VERSION"; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi
  done
  echo "Python $MYSQL_SHELL_WEB_PYTHON_MIN_VERSION or newer is required." >&2
  return 1
}

create_virtualenv() {
  local python_bin="$1"
  if [[ "$python_bin" == *$'\n'* || ! -x "$python_bin" ]]; then
    echo "Selected Python interpreter is invalid: $python_bin" >&2
    return 1
  fi
  if "$python_bin" -m venv "$VENV_DIR"; then
    return 0
  fi
  echo "Virtualenv creation failed. Attempting to install matching venv support and retry." >&2
  if ! privileged_setup_skipped && command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12-venv || true
  fi
  "$python_bin" -m venv "$VENV_DIR"
}

run_dependency_audit() {
  case "$(to_lower "$MYSQL_SHELL_WEB_DEPENDENCY_AUDIT")" in
    off|0|false|no) return 0 ;;
  esac
  "$VENV_DIR/bin/python" -m pip install --upgrade pip-audit >/dev/null 2>&1 || {
    echo "pip-audit could not be installed; continuing." >&2
    return 0
  }
  mkdir -p "$SCRIPT_DIR/.cache/pip-audit"
  if "$VENV_DIR/bin/python" -m pip_audit --cache-dir "$SCRIPT_DIR/.cache/pip-audit" -r "$SCRIPT_DIR/requirements.txt"; then
    return 0
  fi
  if [[ "$(to_lower "$MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT")" =~ ^(1|true|yes|on)$ ]]; then
    return 1
  fi
  echo "Dependency audit reported issues; continuing because strict mode is disabled." >&2
}

mark_local_state_files() {
  if [[ ! -d "$SCRIPT_DIR/.git" ]] || ! command -v git >/dev/null 2>&1; then
    return 0
  fi

  if git -C "$SCRIPT_DIR" ls-files --error-unmatch profiles.json >/dev/null 2>&1; then
    git -C "$SCRIPT_DIR" update-index --skip-worktree profiles.json || true
    echo "Marked profiles.json as local-only for this checkout."
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
  local mysqlsh_binary
  local selected_python

  load_existing_runtime_env
  parse_args "$@"
  os_family="$OS_FAMILY_INPUT"

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

  selected_python="$(select_python_bin "$os_family")"
  MYSQL_SHELL_WEB_PYTHON_BIN="$selected_python"
  create_virtualenv "$selected_python"
  "$VENV_DIR/bin/python" -m pip install --upgrade pip wheel
  "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
  ensure_flask_secret_key
  run_dependency_audit

  mysqlsh_binary="$(run_mysqlsh_installer "$os_family")"
  write_runtime_env "$http_port" "$https_port" "$host_value" "$ssl_cert_file" "$ssl_key_file" "$mysqlsh_binary" "$os_family" "$deploy_mode"
  repair_local_permissions
  ensure_local_mysql_profile "$os_family"
  repair_local_permissions
  setup_local_mysql_systemd_service "$os_family"
  mark_local_state_files
  setup_systemd_services "$os_family" "$deploy_mode" "$ssl_cert_file" "$ssl_key_file" "$http_port" "$https_port"

  if privileged_setup_skipped; then
    echo "Skipping firewall changes because SKIP_PRIVILEGED_SETUP is set."
  else
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
  fi

  echo "Setup completed."
  echo "Virtual environment: $VENV_DIR"
  echo "Python runtime: $selected_python"
  echo "Saved runtime defaults: $RUNTIME_ENV_FILE"
  echo "Default host: $host_value"
  echo "Default HTTP port: $http_port"
  echo "Default HTTPS port: $https_port"
  echo "MySQL Shell binary: $mysqlsh_binary"
  if [[ -n "$ssl_cert_file" && -n "$ssl_key_file" ]]; then
    echo "TLS certificate: $ssl_cert_file"
    echo "TLS key: $ssl_key_file"
  fi
  echo "HTTP start script: $SCRIPT_DIR/start_http.sh"
  echo "HTTPS start script: $SCRIPT_DIR/start_https.sh"
  case "$os_family" in
    ol8|ol9|ubuntu)
      echo "Systemd services: ${APP_SLUG}-local-mysql.service, ${APP_SLUG}-http.service and ${APP_SLUG}-https.service"
      ;;
  esac
  echo "Use PORT=<port> at launch time to override either saved default temporarily."
}

main "$@"
