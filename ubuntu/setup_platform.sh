#!/usr/bin/env bash

platform_install_python_if_possible() {
  run_as_root apt-get update >&2
  run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12 python3.12-venv python3.12-dev >&2 || true
}

platform_install_venv_support() {
  run_as_root apt-get update
  run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12-venv || true
}

platform_install_bind_capability_tool() {
  run_as_root apt-get update
  run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libcap2-bin || true
}

platform_install_embedded_mysql_server_dependencies() {
  run_as_root apt-get update
  run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libaio1 libncurses6 xz-utils || \
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libaio1t64 libncurses6 xz-utils || \
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libaio-dev libncurses6 xz-utils || true
}

platform_prepare_embedded_mysql_runtime_compat() {
  local basedir="$1"
  local compat_lib_dir="$2"
  local libaio_source=""

  if ldd "$basedir/bin/mysqld" 2>/dev/null | grep -q 'libaio\.so\.1 => not found'; then
    libaio_source="$(find /usr/lib /lib \( -name 'libaio.so.1t64' -o -name 'libaio.so.1t64.*' \) 2>/dev/null | head -n 1 || true)"
    if [[ -z "$libaio_source" ]]; then
      echo "Embedded MySQL Server requires libaio.so.1. Install libaio1/libaio1t64 or provide a compatible library." >&2
      return 1
    fi
    mkdir -p "$compat_lib_dir"
    ln -sfn "$libaio_source" "$compat_lib_dir/libaio.so.1"
  fi
}

platform_prepare_local_mysql_security_policy() {
  local local_mysql_cnf="$1"
  local script_dir="$2"
  local profile_file="/etc/apparmor.d/usr.sbin.mysqld"
  local local_file="/etc/apparmor.d/local/usr.sbin.mysqld"

  if [[ ! -f "$profile_file" ]]; then
    echo "Ubuntu AppArmor MySQL profile was not found at $profile_file; skipping app-local MySQL allowance."
    return 0
  fi
  if privileged_setup_skipped; then
    echo "Skipping Ubuntu AppArmor app-local MySQL allowance because SKIP_PRIVILEGED_SETUP is set." >&2
    return 0
  fi

  run_as_root mkdir -p "$(dirname "$local_file")"
  run_as_root touch "$local_file"
  run_as_root sed -i '/^# BEGIN mysql-shell-web app-local MySQL$/,/^# END mysql-shell-web app-local MySQL$/d' "$local_file"
  {
    echo "# BEGIN mysql-shell-web app-local MySQL"
    echo "$local_mysql_cnf r,"
    echo "$script_dir/.embedded/mysql-server/** mr,"
    echo "$script_dir/.data/ rw,"
    echo "$script_dir/.data/** rwk,"
    echo "# END mysql-shell-web app-local MySQL"
  } | run_as_root tee -a "$local_file" >/dev/null

  if command -v apparmor_parser >/dev/null 2>&1; then
    if run_as_root apparmor_parser -r "$profile_file"; then
      echo "Reloaded Ubuntu AppArmor MySQL profile with app-local MySQL allowances."
    else
      echo "Unable to reload Ubuntu AppArmor MySQL profile. Check AppArmor logs if embedded MySQL startup fails." >&2
    fi
  else
    echo "apparmor_parser is not available; check AppArmor logs if embedded MySQL startup fails." >&2
  fi
}

platform_open_firewall_port() {
  local protocol_label="$1"
  local port_value="$2"

  if command -v ufw >/dev/null 2>&1; then
    if run_as_root ufw allow "${port_value}/tcp"; then
      echo "Opened firewall port ${port_value}/tcp for ${protocol_label} with ufw."
      return 0
    fi
    echo "ufw returned an error. Trying iptables for ${port_value}/tcp." >&2
  fi

  if command -v iptables >/dev/null 2>&1; then
    if run_as_root iptables -C INPUT -p tcp -m state --state NEW -m tcp --dport "$port_value" -j ACCEPT 2>/dev/null || \
      run_as_root iptables -I INPUT 5 -p tcp -m state --state NEW -m tcp --dport "$port_value" -j ACCEPT 2>/dev/null || \
      run_as_root iptables -I INPUT 1 -p tcp -m state --state NEW -m tcp --dport "$port_value" -j ACCEPT; then
      echo "Opened firewall port ${port_value}/tcp for ${protocol_label} with iptables."
      if command -v iptables-save >/dev/null 2>&1 && [[ -d /etc/iptables ]]; then
        run_as_root sh -c 'iptables-save > /etc/iptables/rules.v4' || true
      fi
      return 0
    fi
  fi

  echo "Firewall tool not found or failed. Open ${port_value}/tcp for ${protocol_label} manually on this host." >&2
}
