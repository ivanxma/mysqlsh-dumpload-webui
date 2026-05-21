#!/usr/bin/env bash

platform_install_python_if_possible() {
  run_as_root dnf install -y python3.12 python3.12-pip python3.12-devel >&2 || true
}

platform_install_venv_support() {
  return 0
}

platform_install_bind_capability_tool() {
  run_as_root dnf install -y libcap >&2 || true
}

platform_install_embedded_mysql_server_dependencies() {
  run_as_root dnf install -y libaio xz || true
  run_as_root dnf install -y ncurses-compat-libs || true
}

platform_prepare_embedded_mysql_runtime_compat() {
  return 0
}

platform_prepare_local_mysql_security_policy() {
  return 0
}

platform_firewall_cmd() {
  if command -v timeout >/dev/null 2>&1; then
    run_as_root timeout -k 3 30 firewall-cmd "$@"
  else
    run_as_root firewall-cmd "$@"
  fi
}

platform_print_firewall_manual_followup() {
  local port_value="$1"

  echo "Unable to complete OL8 firewalld automation. Run these commands manually after checking firewalld health:" >&2
  echo "  sudo systemctl enable --now firewalld" >&2
  echo "  sudo firewall-cmd --zone=public --permanent --add-port=${port_value}/tcp" >&2
  echo "  sudo firewall-cmd --reload" >&2
  echo "  sudo firewall-cmd --zone=public --list-ports" >&2
  echo "  sudo ss -ltnp | grep ':${port_value}'" >&2
}

platform_firewall_cmd_with_retries() {
  local description="$1"
  shift
  local attempt=1
  local max_attempts="${MYSQL_SHELL_WEB_FIREWALL_RETRY_ATTEMPTS:-3}"
  local delay_seconds="${MYSQL_SHELL_WEB_FIREWALL_RETRY_DELAY:-20}"

  while [[ "$attempt" -le "$max_attempts" ]]; do
    if platform_firewall_cmd "$@"; then
      return 0
    fi
    echo "OL8 firewalld ${description} failed on attempt ${attempt}/${max_attempts}." >&2
    if [[ "$attempt" -lt "$max_attempts" ]]; then
      echo "Retrying OL8 firewalld ${description} after ${delay_seconds}s." >&2
      sleep "$delay_seconds"
    fi
    attempt=$((attempt + 1))
  done

  return 1
}

platform_open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"

  if ! command -v systemctl >/dev/null 2>&1 || ! command -v firewall-cmd >/dev/null 2>&1; then
    echo "systemctl and firewall-cmd are required to open ${port_value}/tcp on OL8." >&2
    return 1
  fi

  echo "Checking OL8 firewalld status."
  run_as_root systemctl status firewalld --no-pager || true
  echo "Starting and enabling OL8 firewalld."
  run_as_root systemctl enable --now firewalld

  echo "Opening raw TCP port ${port_value}/tcp in OL8 firewalld zone: public"
  if ! platform_firewall_cmd_with_retries "permanent port update" --zone=public --permanent --add-port="${port_value}/tcp"; then
    platform_print_firewall_manual_followup "$port_value"
    return 0
  fi

  echo "Reloading OL8 firewalld."
  if ! platform_firewall_cmd_with_retries "reload" --reload; then
    platform_print_firewall_manual_followup "$port_value"
    return 0
  fi
  echo "Verifying OL8 firewalld services:"
  platform_firewall_cmd --zone=public --list-services || true
  echo "Verifying OL8 firewalld ports:"
  platform_firewall_cmd --zone=public --list-ports || true
  echo "Verifying OL8 firewalld zone details:"
  platform_firewall_cmd --zone=public --list-all || true
  echo "Verifying app listener on ${port_value}:"
  ss -ltnp 2>/dev/null | grep ":${port_value}" || true
}
