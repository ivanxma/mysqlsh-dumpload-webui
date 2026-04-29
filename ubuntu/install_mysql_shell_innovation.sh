#!/usr/bin/env bash
set -euo pipefail

run_root() {
  if [[ $EUID -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    echo "Run this script as root or install sudo." >&2
    return 1
  fi
}

if command -v mysqlsh >/dev/null 2>&1; then
  echo "mysqlsh is already installed."
  exit 0
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "apt-get is required on Ubuntu but was not found." >&2
  exit 1
fi

run_root apt-get update
if ! run_root env DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-shell; then
  echo "Unable to install mysql-shell via apt on Ubuntu." >&2
  echo "Add the MySQL APT repository for the innovation release, then rerun this script." >&2
  exit 1
fi

echo "mysqlsh installed successfully."
