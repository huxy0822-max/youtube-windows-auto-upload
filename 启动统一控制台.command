#!/bin/zsh

set -e

REPO_DIR="/Users/huxy/youtube-auto"
PYTHON_CANDIDATES=(
  "$REPO_DIR/.venv/bin/python3"
  "/opt/homebrew/bin/python3.14"
  "/opt/homebrew/bin/python3"
  "/usr/local/bin/python3"
  "/usr/bin/python3"
)

cd "$REPO_DIR"

PYTHON_BIN=""
for candidate in "${PYTHON_CANDIDATES[@]}"; do
  if [ -x "$candidate" ]; then
    PYTHON_BIN="$candidate"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "未找到可用 Python。建议先安装 Homebrew Python："
  echo "brew install python@3.14 python-tk@3.14"
  exit 1
fi

if [ ! -x "$REPO_DIR/.venv/bin/python3" ]; then
  echo "正在创建本地虚拟环境：$REPO_DIR/.venv"
  "$PYTHON_BIN" -m venv "$REPO_DIR/.venv"
fi

export PATH="$REPO_DIR/.venv/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

echo "启动仓库：$REPO_DIR"
echo "当前分支：$(git branch --show-current 2>/dev/null || echo unknown)"
echo "当前提交：$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

"$REPO_DIR/.venv/bin/python3" -m pip install -q -r "$REPO_DIR/requirements.txt"
"$REPO_DIR/.venv/bin/python3" "$REPO_DIR/dashboard.py"
