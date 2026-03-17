#!/bin/zsh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

choose_python() {
  local candidates=(
    "/opt/homebrew/bin/python3"
    "python3.14"
    "python3.13"
    "python3.12"
    "python3.11"
    "python3.10"
    "python3"
  )

  for candidate in "${candidates[@]}"; do
    if ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi
    if "$candidate" -c 'import tkinter as tk; root = tk.Tk(); root.withdraw(); root.destroy()' >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

if ! PYTHON_BIN="$(choose_python)"; then
  cat <<'EOF'
未找到可用的 Tk Python，统一控制台暂时无法启动。

建议先执行：
  brew install python@3.14 python-tk@3.14

然后重新双击本文件。
EOF
  exit 1
fi

VENV_DIR="$SCRIPT_DIR/.venv"
VENV_PY="$VENV_DIR/bin/python"
VENV_PIP="$VENV_DIR/bin/pip"

if [ ! -x "$VENV_PY" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if ! "$VENV_PY" -c 'import customtkinter, PIL, requests, playwright' >/dev/null 2>&1; then
  "$VENV_PIP" install -r "$SCRIPT_DIR/requirements.txt"
fi

exec "$VENV_PY" "$SCRIPT_DIR/dashboard.py"
