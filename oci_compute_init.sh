#!/bin/bash
set -euo pipefail

APP_TITLE="${APP_TITLE:-MySQL Shell Web}"
APP_REPO="${APP_REPO:-https://github.com/ivanxma/mysqlsh-dumpload-webui.git}"
APP_BRANCH="${APP_BRANCH:-main}"
OS_FAMILY="${OS_FAMILY:-ol9}"
DEPLOY_MODE="${DEPLOY_MODE:-https}"
HTTP_PORT="${HTTP_PORT:-80}"
HTTPS_PORT="${HTTPS_PORT:-443}"
APP_SLUG="${APP_SLUG:-mysql-shell-web}"
HOST="${HOST:-0.0.0.0}"
LOCAL_MYSQL_BOOTSTRAP="${LOCAL_MYSQL_BOOTSTRAP:-1}"
LOCAL_MYSQL_PROFILE_NAME="${LOCAL_MYSQL_PROFILE_NAME:-local-admin-profile}"
LOCAL_MYSQL_ADMIN_USER="${LOCAL_MYSQL_ADMIN_USER:-localadmin}"
LOCAL_MYSQL_ADMIN_PASSWORD="${LOCAL_MYSQL_ADMIN_PASSWORD:-}"
LOCAL_MYSQL_DATABASE="${LOCAL_MYSQL_DATABASE:-mysql}"
MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES="${MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES:-9}"
MYSQL_SERVER_EMBEDDED_VERSION="${MYSQL_SERVER_EMBEDDED_VERSION:-9.7.0}"
MYSQL_SERVER_BRIDGE_VERSION="${MYSQL_SERVER_BRIDGE_VERSION:-8.4.8}"
MYSQL_SERVER_URL_LINUX_X86="${MYSQL_SERVER_URL_LINUX_X86:-}"
MYSQL_SERVER_URL_LINUX_ARM="${MYSQL_SERVER_URL_LINUX_ARM:-}"
MYSQL_SERVER_BRIDGE_URL_LINUX_X86="${MYSQL_SERVER_BRIDGE_URL_LINUX_X86:-}"
MYSQL_SERVER_BRIDGE_URL_LINUX_ARM="${MYSQL_SERVER_BRIDGE_URL_LINUX_ARM:-}"
MYSQL_SERVER_RUNTIME_DIR="${MYSQL_SERVER_RUNTIME_DIR:-}"
MYSQL_SERVER_DOWNLOADS_DIR="${MYSQL_SERVER_DOWNLOADS_DIR:-}"
SSL_CERT_FILE="${SSL_CERT_FILE:-}"
SSL_KEY_FILE="${SSL_KEY_FILE:-}"

case "$OS_FAMILY" in
  ol8|ol9)
    DEFAULT_APP_USER="opc"
    ;;
  ubuntu)
    DEFAULT_APP_USER="ubuntu"
    ;;
  *)
    echo "Unsupported OS_FAMILY: $OS_FAMILY. Expected ol8, ol9, or ubuntu." >&2
    exit 1
    ;;
esac

APP_USER="${APP_USER:-$DEFAULT_APP_USER}"
APP_GROUP="${APP_GROUP:-$APP_USER}"
APP_DIR="${APP_DIR:-/home/$APP_USER/mysqlsh-dumpload-webui}"
LOCAL_MYSQL_SOCKET="${LOCAL_MYSQL_SOCKET:-$APP_DIR/.data/run/mysql.sock}"

case "$DEPLOY_MODE" in
  http)
    DEFAULT_SERVICE_NAME="${APP_SLUG}-http.service"
    ;;
  https|both)
    DEFAULT_SERVICE_NAME="${APP_SLUG}-https.service"
    ;;
  none)
    DEFAULT_SERVICE_NAME="${APP_SLUG}-local-mysql.service"
    ;;
  *)
    echo "Unsupported DEPLOY_MODE: $DEPLOY_MODE. Expected http, https, both, or none." >&2
    exit 1
    ;;
esac
SERVICE_NAME="${SERVICE_NAME:-$DEFAULT_SERVICE_NAME}"

STATE_DIR="/var/lib/${APP_SLUG}-init"
INSTALLING_FLAG="$STATE_DIR/installing"
INSTALLED_FLAG="$STATE_DIR/installed"
FAILED_FLAG="$STATE_DIR/failed"
SERVICE_FILE="$STATE_DIR/service-name"
LOG_FILE="/var/log/${APP_SLUG}-init.log"
PROFILE_BANNER="/etc/profile.d/${APP_SLUG}-login-banner.sh"

mkdir -p "$STATE_DIR"
chmod 0755 "$STATE_DIR"
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

touch "$INSTALLING_FLAG"
rm -f "$INSTALLED_FLAG" "$FAILED_FLAG"
printf '%s\n' "$SERVICE_NAME" > "$SERVICE_FILE"
chmod 0644 "$SERVICE_FILE"

cat > "$PROFILE_BANNER" <<BANNER
#!/bin/bash
STATE_DIR="$STATE_DIR"
INSTALLING_FLAG="\$STATE_DIR/installing"
INSTALLED_FLAG="\$STATE_DIR/installed"
FAILED_FLAG="\$STATE_DIR/failed"
SERVICE_FILE="\$STATE_DIR/service-name"
LOG_FILE="$LOG_FILE"

case \$- in
  *i*) ;;
  *) return 0 ;;
esac

[ "\${USER:-}" = "$APP_USER" ] || return 0

SERVICE_NAME=""
if [ -r "\$SERVICE_FILE" ]; then
  SERVICE_NAME="\$(head -n 1 "\$SERVICE_FILE")"
fi

show_service_status() {
  [ -n "\$SERVICE_NAME" ] || return 0
  if systemctl list-unit-files "\$SERVICE_NAME" --no-legend 2>/dev/null | grep -Fq "\$SERVICE_NAME"; then
    systemctl --no-pager --full --lines=12 status "\$SERVICE_NAME" || true
  else
    printf '%s\\n' "$APP_TITLE service unit has not been created yet."
  fi
}

printf '\\n'
if [ -f "\$INSTALLING_FLAG" ]; then
  printf '%s\\n' "Please wait until installation to be completed."
elif [ -f "\$INSTALLED_FLAG" ]; then
  printf '%s\\n' "$APP_TITLE setup has been completed"
  show_service_status
elif [ -f "\$FAILED_FLAG" ]; then
  printf '%s\\n' "The installation finished with errors. Recent setup log:"
  tail -n 30 "\$LOG_FILE" 2>/dev/null || true
  show_service_status
fi
printf '\\n'
BANNER
chmod 0755 "$PROFILE_BANNER"

finish_install() {
  local exit_code="$1"
  rm -f "$INSTALLING_FLAG"
  if [ "$exit_code" -eq 0 ]; then
    touch "$INSTALLED_FLAG"
    rm -f "$FAILED_FLAG"
  else
    touch "$FAILED_FLAG"
    rm -f "$INSTALLED_FLAG"
  fi
}

trap 'finish_install $?' EXIT

install_package_prereqs() {
  case "$OS_FAMILY" in
    ol8|ol9)
      if command -v dnf >/dev/null 2>&1; then
        dnf install -y curl git xz libaio || true
        dnf install -y ncurses-compat-libs || true
      elif command -v yum >/dev/null 2>&1; then
        yum install -y curl git xz libaio || true
        yum install -y ncurses-compat-libs || true
      else
        echo "Unable to install prerequisites automatically on $OS_FAMILY." >&2
        return 1
      fi
      ;;
    ubuntu)
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y curl git xz-utils libaio1 libncurses6 || \
        DEBIAN_FRONTEND=noninteractive apt-get install -y curl git xz-utils libaio1t64 libncurses6 || \
        DEBIAN_FRONTEND=noninteractive apt-get install -y curl git xz-utils libaio-dev libncurses6 || true
      ;;
  esac

  if ! command -v git >/dev/null 2>&1; then
    echo "git is required but was not installed successfully." >&2
    return 1
  fi
}

ensure_app_user() {
  if ! getent group "$APP_GROUP" >/dev/null 2>&1; then
    groupadd "$APP_GROUP"
  fi

  if ! id "$APP_USER" >/dev/null 2>&1; then
    useradd --create-home --shell /bin/bash --gid "$APP_GROUP" "$APP_USER"
  fi
}

run_as_app_user() {
  if [ "$(id -u "$APP_USER")" = "$(id -u)" ]; then
    "$@"
  else
    sudo -u "$APP_USER" "$@"
  fi
}

checkout_app() {
  mkdir -p "$(dirname "$APP_DIR")"
  chown "$APP_USER:$APP_GROUP" "$(dirname "$APP_DIR")"

  if [ -d "$APP_DIR/.git" ]; then
    run_as_app_user git -C "$APP_DIR" remote set-url origin "$APP_REPO"
    run_as_app_user git -C "$APP_DIR" fetch --all --prune
    run_as_app_user git -C "$APP_DIR" checkout -B "$APP_BRANCH" "origin/$APP_BRANCH"
    run_as_app_user git -C "$APP_DIR" pull --ff-only origin "$APP_BRANCH"
  elif [ -e "$APP_DIR" ]; then
    mv "$APP_DIR" "${APP_DIR}.$(date +%Y%m%d%H%M%S)"
    run_as_app_user git clone --branch "$APP_BRANCH" --single-branch "$APP_REPO" "$APP_DIR"
  else
    run_as_app_user git clone --branch "$APP_BRANCH" --single-branch "$APP_REPO" "$APP_DIR"
  fi
}

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this init script as root. OCI initialization scripts run as root by default." >&2
  exit 1
fi

install_package_prereqs
ensure_app_user
checkout_app
cd "$APP_DIR"

if [ "$LOCAL_MYSQL_BOOTSTRAP" = "1" ] && [ -z "$LOCAL_MYSQL_ADMIN_PASSWORD" ]; then
  echo "LOCAL_MYSQL_ADMIN_PASSWORD must be provided for first-boot local-admin-profile bootstrap. Refusing to generate or log a password automatically." >&2
  exit 1
fi

SETUP_ARGS=( "$OS_FAMILY" "$DEPLOY_MODE" )
if [ -n "$HTTP_PORT" ]; then
  SETUP_ARGS+=( "--http-port" "$HTTP_PORT" )
fi
if [ -n "$HTTPS_PORT" ]; then
  SETUP_ARGS+=( "--https-port" "$HTTPS_PORT" )
fi

run_as_app_user env \
  HOST="$HOST" \
  SERVICE_USER="$APP_USER" \
  SERVICE_GROUP="$APP_GROUP" \
  SSL_CERT_FILE="$SSL_CERT_FILE" \
  SSL_KEY_FILE="$SSL_KEY_FILE" \
  LOCAL_MYSQL_PROFILE_NAME="$LOCAL_MYSQL_PROFILE_NAME" \
  LOCAL_MYSQL_ADMIN_USER="$LOCAL_MYSQL_ADMIN_USER" \
  LOCAL_MYSQL_ADMIN_PASSWORD="$LOCAL_MYSQL_ADMIN_PASSWORD" \
  LOCAL_MYSQL_DATABASE="$LOCAL_MYSQL_DATABASE" \
  LOCAL_MYSQL_SOCKET="$LOCAL_MYSQL_SOCKET" \
  MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES="$MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES" \
  MYSQL_SERVER_EMBEDDED_VERSION="$MYSQL_SERVER_EMBEDDED_VERSION" \
  MYSQL_SERVER_BRIDGE_VERSION="$MYSQL_SERVER_BRIDGE_VERSION" \
  MYSQL_SERVER_URL_LINUX_X86="$MYSQL_SERVER_URL_LINUX_X86" \
  MYSQL_SERVER_URL_LINUX_ARM="$MYSQL_SERVER_URL_LINUX_ARM" \
  MYSQL_SERVER_BRIDGE_URL_LINUX_X86="$MYSQL_SERVER_BRIDGE_URL_LINUX_X86" \
  MYSQL_SERVER_BRIDGE_URL_LINUX_ARM="$MYSQL_SERVER_BRIDGE_URL_LINUX_ARM" \
  MYSQL_SERVER_RUNTIME_DIR="$MYSQL_SERVER_RUNTIME_DIR" \
  MYSQL_SERVER_DOWNLOADS_DIR="$MYSQL_SERVER_DOWNLOADS_DIR" \
  bash ./setup.sh "${SETUP_ARGS[@]}"

if command -v systemctl >/dev/null 2>&1; then
  systemctl --no-pager --full --lines=12 status "$SERVICE_NAME" || true
  if [ "$LOCAL_MYSQL_BOOTSTRAP" = "1" ]; then
    systemctl --no-pager --full --lines=12 status "${APP_SLUG}-local-mysql.service" || true
  fi
fi
