#!/usr/bin/env bash

platform_install_python_if_possible() {
  if command -v brew >/dev/null 2>&1; then
    brew install python@3.12 >&2 || true
  fi
}

platform_install_venv_support() {
  return 0
}

platform_install_bind_capability_tool() {
  return 0
}

platform_install_embedded_mysql_server_dependencies() {
  return 0
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
  echo "macOS does not expose Linux-style port opening here. Allow the Python process through the macOS firewall if prompted, or open ${port_value}/tcp for ${protocol_label} manually." >&2
}
