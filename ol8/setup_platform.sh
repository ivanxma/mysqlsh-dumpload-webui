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
    run_as_root timeout 20 firewall-cmd "$@"
  else
    run_as_root firewall-cmd "$@"
  fi
}

platform_open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"
  local zone=""
  local active_zones=""
  local zone_attempt

  if ! command -v systemctl >/dev/null 2>&1 || ! command -v firewall-cmd >/dev/null 2>&1; then
    echo "systemctl and firewall-cmd are required to open ${port_value}/tcp on OL8." >&2
    return 1
  fi

  echo "Checking OL8 firewalld status."
  run_as_root systemctl status firewalld --no-pager || true
  echo "Starting and enabling OL8 firewalld."
  run_as_root systemctl enable --now firewalld
  sleep 2
  echo "OL8 active firewalld zones:"
  for zone_attempt in 1 2 3 4 5 6; do
    if active_zones="$(platform_firewall_cmd --get-active-zones)"; then
      printf '%s\n' "$active_zones"
      zone="$(printf '%s\n' "$active_zones" | awk 'NR == 1 { print $1 }')"
      if [[ -n "$zone" ]]; then
        break
      fi
    fi
    if [[ "$zone_attempt" != "6" ]]; then
      echo "Unable to read OL8 active firewalld zone; retrying ${zone_attempt}/6." >&2
      run_as_root systemctl restart firewalld || true
      sleep 5
    fi
  done

  if [[ -z "$zone" ]]; then
    echo "Unable to resolve an active OL8 firewalld zone from firewall-cmd --get-active-zones." >&2
    return 1
  fi

  if [[ "$protocol_label" == "HTTPS" && "$port_value" == "443" ]]; then
    echo "Opening standard HTTPS service in OL8 firewalld zone: $zone"
    platform_firewall_cmd --zone="$zone" --permanent --add-service=https
  else
    echo "Opening raw TCP port ${port_value}/tcp in OL8 firewalld zone: $zone"
    platform_firewall_cmd --zone="$zone" --permanent --add-port="${port_value}/tcp"
  fi

  echo "Reloading OL8 firewalld."
  platform_firewall_cmd --reload
  echo "Verifying OL8 firewalld services:"
  platform_firewall_cmd --zone="$zone" --list-services || true
  echo "Verifying OL8 firewalld ports:"
  platform_firewall_cmd --zone="$zone" --list-ports || true
  echo "Verifying OL8 firewalld zone details:"
  platform_firewall_cmd --zone="$zone" --list-all || true
  echo "Verifying app listener on ${port_value}:"
  ss -ltnp 2>/dev/null | grep ":${port_value}" || true
}
