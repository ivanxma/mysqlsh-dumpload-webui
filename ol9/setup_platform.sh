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
  local zone="${2:-public}"

  echo "Unable to complete OL9 firewalld automation. Run these commands manually after checking firewalld health:" >&2
  echo "  sudo systemctl enable --now firewalld" >&2
  echo "  sudo firewall-cmd --get-active-zones" >&2
  echo "  sudo firewall-cmd --zone=${zone} --permanent --add-port=${port_value}/tcp" >&2
  echo "  sudo firewall-cmd --reload" >&2
  echo "  sudo firewall-cmd --zone=${zone} --list-ports" >&2
  echo "  sudo ss -ltnp | grep ':${port_value}'" >&2
}

platform_resolve_firewalld_zone() {
  local active_zones=""
  local zone=""

  active_zones="$(platform_firewall_cmd --get-active-zones 2>/dev/null || true)"
  zone="$(printf '%s\n' "$active_zones" | awk 'NR == 1 { print $1 }')"
  if [[ -n "$zone" ]]; then
    printf '%s\n' "$active_zones" >&2
    printf '%s' "$zone"
    return 0
  fi

  zone="$(platform_firewall_cmd --get-default-zone 2>/dev/null || true)"
  zone="$(printf '%s\n' "$zone" | awk 'NR == 1 { print $1 }')"
  if [[ -n "$zone" ]]; then
    echo "Using OL9 default firewalld zone because no active zone was reported: $zone" >&2
    printf '%s' "$zone"
    return 0
  fi

  echo "Using OL9 firewalld zone public because no active or default zone was reported." >&2
  printf '%s' "public"
  return 0
}

platform_open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"
  local zone=""

  if ! command -v systemctl >/dev/null 2>&1 || ! command -v firewall-cmd >/dev/null 2>&1; then
    echo "systemctl and firewall-cmd are required to open ${port_value}/tcp on OL9." >&2
    return 1
  fi

  echo "Checking OL9 firewalld status."
  run_as_root systemctl status firewalld --no-pager || true
  echo "Starting and enabling OL9 firewalld."
  run_as_root systemctl enable --now firewalld
  echo "OL9 active firewalld zones:"
  zone="$(platform_resolve_firewalld_zone || true)"

  if [[ -z "$zone" ]]; then
    platform_print_firewall_manual_followup "$port_value" "public"
    return 0
  fi

  echo "Opening raw TCP port ${port_value}/tcp in OL9 firewalld zone: $zone"
  if ! platform_firewall_cmd --zone="$zone" --permanent --add-port="${port_value}/tcp"; then
    platform_print_firewall_manual_followup "$port_value" "$zone"
    return 0
  fi
  echo "Reloading OL9 firewalld."
  if ! platform_firewall_cmd --reload; then
    platform_print_firewall_manual_followup "$port_value" "$zone"
    return 0
  fi
  echo "Verifying OL9 firewalld services:"
  platform_firewall_cmd --zone="$zone" --list-services || true
  echo "Verifying OL9 firewalld ports:"
  platform_firewall_cmd --zone="$zone" --list-ports || true
  echo "Verifying OL9 firewalld zone details:"
  platform_firewall_cmd --zone="$zone" --list-all || true
  echo "Verifying app listener on ${port_value}:"
  ss -ltnp 2>/dev/null | grep ":${port_value}" || true
}
