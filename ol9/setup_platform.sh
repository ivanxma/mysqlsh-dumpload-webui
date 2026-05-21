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

platform_open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"
  local zone=""
  local active_zones=""
  local zone_attempt

  if ! command -v systemctl >/dev/null 2>&1 || ! command -v firewall-cmd >/dev/null 2>&1; then
    echo "systemctl and firewall-cmd are required to open ${port_value}/tcp on OL9." >&2
    return 1
  fi

  echo "Checking OL9 firewalld status."
  run_as_root systemctl status firewalld --no-pager || true
  echo "Starting and enabling OL9 firewalld."
  run_as_root systemctl enable --now firewalld
  echo "OL9 active firewalld zones:"
  for zone_attempt in 1 2 3 4 5 6; do
    if active_zones="$(run_as_root firewall-cmd --get-active-zones)"; then
      printf '%s\n' "$active_zones"
      zone="$(printf '%s\n' "$active_zones" | awk 'NR == 1 { print $1 }')"
      if [[ -n "$zone" ]]; then
        break
      fi
    fi
    if [[ "$zone_attempt" != "6" ]]; then
      echo "Unable to read OL9 active firewalld zone; retrying ${zone_attempt}/6." >&2
      sleep 10
    fi
  done

  if [[ -z "$zone" ]]; then
    echo "Unable to resolve an active OL9 firewalld zone from firewall-cmd --get-active-zones." >&2
    return 1
  fi

  if [[ "$protocol_label" == "HTTPS" && "$port_value" == "443" ]]; then
    echo "Opening standard HTTPS service in OL9 firewalld zone: $zone"
    run_as_root firewall-cmd --zone="$zone" --permanent --add-service=https
  else
    echo "Opening raw TCP port ${port_value}/tcp in OL9 firewalld zone: $zone"
    run_as_root firewall-cmd --zone="$zone" --permanent --add-port="${port_value}/tcp"
  fi
  echo "Reloading OL9 firewalld."
  run_as_root firewall-cmd --reload
  echo "Verifying OL9 firewalld services:"
  run_as_root firewall-cmd --zone="$zone" --list-services || true
  echo "Verifying OL9 firewalld ports:"
  run_as_root firewall-cmd --zone="$zone" --list-ports || true
  echo "Verifying OL9 firewalld zone details:"
  run_as_root firewall-cmd --zone="$zone" --list-all || true
  echo "Verifying app listener on ${port_value}:"
  ss -ltnp 2>/dev/null | grep ":${port_value}" || true
}
