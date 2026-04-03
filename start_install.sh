#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
COLLECT_INTERVAL="${COLLECT_INTERVAL:-30}"
DEFAULT_NODE_IP="127.0.0.1"

cleanup() {
  if [[ -n "${COLLECTOR_PID:-}" ]]; then kill "$COLLECTOR_PID" 2>/dev/null || true; fi
  if [[ -n "${BACKEND_PID:-}" ]]; then kill "$BACKEND_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

ensure_line() {
  local file="$1"
  local key="$2"
  local value="$3"
  if grep -qE "^${key}=" "$file"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

read -r -p "Node IP or host [${DEFAULT_NODE_IP}]: " NODE_IP
NODE_IP="${NODE_IP:-$DEFAULT_NODE_IP}"

cd "$BACKEND_DIR"
python3 -m venv .venv
VENV_PY="$BACKEND_DIR/.venv/bin/python"
VENV_PIP="$BACKEND_DIR/.venv/bin/pip"
VENV_UVICORN="$BACKEND_DIR/.venv/bin/uvicorn"
"$VENV_PIP" install -q -r requirements.txt

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

ensure_line .env OPS_BASE_URL "http://${NODE_IP}:9153"
ensure_line .env LATENCY_TARGET_URL "http://${NODE_IP}:9153/health"

echo "Configured backend to use node host: ${NODE_IP}"
"$VENV_PY" history_sqlite.py

(
  while true; do
    "$VENV_PY" collector_sqlite.py || true
    sleep "$COLLECT_INTERVAL"
  done
) &
COLLECTOR_PID=$!

echo "Collector loop started with PID ${COLLECTOR_PID}"
"$VENV_UVICORN" main:app --host 0.0.0.0 --port "$BACKEND_PORT" --reload &
BACKEND_PID=$!

echo "Backend started on http://127.0.0.1:${BACKEND_PORT}"

cd "$FRONTEND_DIR"
npm install --silent

echo "Frontend starting on http://127.0.0.1:${FRONTEND_PORT}"
echo "Press Ctrl+C to stop frontend, backend, and collector."
npm run dev -- --host 0.0.0.0 --port "$FRONTEND_PORT"
