#!/bin/bash
set -euo pipefail

APP_REPO="${APP_REPO:-https://github.com/ivanxma/mysqlsh-dumpload-webui.git}"
OS_FAMILY="${OS_FAMILY:-ol9}"
case "$OS_FAMILY" in
  ubuntu)
    DEFAULT_APP_USER="ubuntu"
    ;;
  ol8|ol9)
    DEFAULT_APP_USER="opc"
    ;;
  *)
    DEFAULT_APP_USER="opc"
    ;;
esac
APP_USER="${APP_USER:-$DEFAULT_APP_USER}"
APP_GROUP="${APP_GROUP:-$APP_USER}"
APP_DIR="${APP_DIR:-/home/${APP_USER}/mysqlsh-dumpload-webui}"
DEPLOY_MODE="${DEPLOY_MODE:-https}"
HTTP_PORT="${HTTP_PORT:-80}"
HTTPS_PORT="${HTTPS_PORT:-443}"
APP_SLUG="${APP_SLUG:-mysql-shell-web}"
APP_TITLE="${APP_TITLE:-MySQL Shell Web}"
SERVICE_NAME="${SERVICE_NAME:-${APP_SLUG}-https.service}"
HOST="${HOST:-0.0.0.0}"
LOCAL_MYSQL_BOOTSTRAP="${LOCAL_MYSQL_BOOTSTRAP:-1}"
LOCAL_MYSQL_PROFILE_NAME="${LOCAL_MYSQL_PROFILE_NAME:-local-admin-profile}"
LOCAL_MYSQL_ADMIN_USER="${LOCAL_MYSQL_ADMIN_USER:-localadmin}"
LOCAL_MYSQL_ADMIN_PASSWORD="${LOCAL_MYSQL_ADMIN_PASSWORD:-}"
LOCAL_MYSQL_DATABASE="${LOCAL_MYSQL_DATABASE:-mysql}"
LOCAL_MYSQL_SOCKET="${LOCAL_MYSQL_SOCKET:-$APP_DIR/.data/run/mysql.sock}"
MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES="${MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES:-9}"
MYSQL_SERVER_EMBEDDED_VERSION="${MYSQL_SERVER_EMBEDDED_VERSION:-9.7.0}"
MYSQL_SERVER_BRIDGE_VERSION="${MYSQL_SERVER_BRIDGE_VERSION:-8.4.8}"
MYSQL_SERVER_URL_LINUX_X86="${MYSQL_SERVER_URL_LINUX_X86:-}"
MYSQL_SERVER_URL_LINUX_ARM="${MYSQL_SERVER_URL_LINUX_ARM:-}"
MYSQL_SERVER_BRIDGE_URL_LINUX_X86="${MYSQL_SERVER_BRIDGE_URL_LINUX_X86:-}"
MYSQL_SERVER_BRIDGE_URL_LINUX_ARM="${MYSQL_SERVER_BRIDGE_URL_LINUX_ARM:-}"
MYSQL_SERVER_RUNTIME_DIR="${MYSQL_SERVER_RUNTIME_DIR:-}"
MYSQL_SERVER_DOWNLOADS_DIR="${MYSQL_SERVER_DOWNLOADS_DIR:-}"

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

cat > "$PROFILE_BANNER" <<EOF
#!/bin/bash
STATE_DIR="$STATE_DIR"
INSTALLING_FLAG="\$STATE_DIR/installing"
INSTALLED_FLAG="\$STATE_DIR/installed"
FAILED_FLAG="\$STATE_DIR/failed"
SERVICE_FILE="\$STATE_DIR/service-name"
LOG_FILE="$LOG_FILE"
LOGIN_USER="$APP_USER"
APP_TITLE="$APP_TITLE"

case \$- in
  *i*) ;;
  *) return 0 ;;
esac

[ "\${USER:-}" = "\$LOGIN_USER" ] || return 0

SERVICE_NAME=""
if [ -r "\$SERVICE_FILE" ]; then
  SERVICE_NAME="\$(head -n 1 "\$SERVICE_FILE")"
fi

printf '\n'
if [ -f "\$INSTALLING_FLAG" ]; then
  printf '%s\n' "Please wait until installation to be completed."
elif [ -f "\$INSTALLED_FLAG" ]; then
  printf '%s\n' "\$APP_TITLE setup has been completed"
  if [ -n "\$SERVICE_NAME" ] && command -v systemctl >/dev/null 2>&1; then
    systemctl --no-pager --full --lines=12 status "\$SERVICE_NAME" || true
  fi
elif [ -f "\$FAILED_FLAG" ]; then
  printf '%s\n' "The installation finished with errors. Review \$LOG_FILE."
  if [ -n "\$SERVICE_NAME" ] && command -v systemctl >/dev/null 2>&1; then
    systemctl --no-pager --full --lines=12 status "\$SERVICE_NAME" || true
  fi
fi
printf '\n'
EOF
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

ensure_user() {
  if id "$APP_USER" >/dev/null 2>&1; then
    return 0
  fi
  useradd --create-home --shell /bin/bash "$APP_USER"
}

install_git() {
  if command -v git >/dev/null 2>&1; then
    return 0
  fi

  if command -v dnf >/dev/null 2>&1; then
    dnf install -y git
  elif command -v yum >/dev/null 2>&1; then
    yum install -y git
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y git
  else
    echo "Unable to install git automatically." >&2
    exit 1
  fi
}

install_embedded_mysql_prerequisites() {
  case "$OS_FAMILY" in
    ol8|ol9)
      if command -v dnf >/dev/null 2>&1; then
        dnf install -y xz libaio || true
        dnf install -y ncurses-compat-libs || true
      elif command -v yum >/dev/null 2>&1; then
        yum install -y xz libaio || true
        yum install -y ncurses-compat-libs || true
      fi
      ;;
    ubuntu)
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y xz-utils libaio1 libncurses6 || \
        DEBIAN_FRONTEND=noninteractive apt-get install -y xz-utils libaio1t64 libncurses6 || \
        DEBIAN_FRONTEND=noninteractive apt-get install -y xz-utils libaio-dev libncurses6 || true
      ;;
  esac
}

ensure_user
install_git
install_embedded_mysql_prerequisites

if [ "$LOCAL_MYSQL_BOOTSTRAP" = "1" ] && [ -z "$LOCAL_MYSQL_ADMIN_PASSWORD" ]; then
  echo "LOCAL_MYSQL_ADMIN_PASSWORD must be explicitly set for first-boot local-admin-profile bootstrap." >&2
  echo "The password is passed only in the setup environment and is not written to the init state files." >&2
  exit 1
fi

parent_dir="$(dirname "$APP_DIR")"
mkdir -p "$parent_dir"
chown "$APP_USER:$APP_GROUP" "$parent_dir"

if [ -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git -C "$APP_DIR" fetch --all --prune
  sudo -u "$APP_USER" git -C "$APP_DIR" pull --ff-only
elif [ -e "$APP_DIR" ]; then
  mv "$APP_DIR" "${APP_DIR}.$(date +%Y%m%d%H%M%S)"
  sudo -u "$APP_USER" git clone "$APP_REPO" "$APP_DIR"
else
  sudo -u "$APP_USER" git clone "$APP_REPO" "$APP_DIR"
fi

cd "$APP_DIR"

setup_args=( "$OS_FAMILY" "$DEPLOY_MODE" )
if [ -n "$HTTP_PORT" ]; then
  setup_args+=( "--http-port" "$HTTP_PORT" )
fi
if [ -n "$HTTPS_PORT" ]; then
  setup_args+=( "--https-port" "$HTTPS_PORT" )
fi

sudo -u "$APP_USER" env \
  HOST="$HOST" \
  SERVICE_USER="$APP_USER" \
  SERVICE_GROUP="$APP_GROUP" \
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
  bash ./setup.sh "${setup_args[@]}"

if command -v systemctl >/dev/null 2>&1; then
  systemctl --no-pager --full --lines=12 status "$SERVICE_NAME" || true
fi
