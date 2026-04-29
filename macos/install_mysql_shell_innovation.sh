#!/usr/bin/env bash
set -euo pipefail

if command -v mysqlsh >/dev/null 2>&1; then
  echo "mysqlsh is already installed."
  exit 0
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This installer is intended for macOS." >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to install mysql-shell on macOS." >&2
  echo "Install Homebrew first, then rerun this script." >&2
  exit 1
fi

if ! brew list mysql-shell >/dev/null 2>&1; then
  brew install mysql-shell
else
  brew upgrade mysql-shell || true
fi

echo "mysqlsh installed successfully."
