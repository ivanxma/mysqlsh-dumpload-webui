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

if ! command -v dnf >/dev/null 2>&1; then
  echo "dnf is required on OL9 but was not found." >&2
  exit 1
fi

if ! run_root dnf install -y mysql-shell; then
  echo "Unable to install mysql-shell via dnf on OL9." >&2
  echo "Enable the MySQL community innovation repository for Oracle Linux 9, then rerun this script." >&2
  exit 1
fi

echo "mysqlsh installed successfully."
