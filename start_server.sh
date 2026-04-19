#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

is_port_in_use() {
  local port="$1"
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
}

if [[ -n "${PORT:-}" ]]; then
  export PORT
else
  for candidate in 3000 3001 3002 3003 3004 3005 3006 3007 3008 3009 3010; do
    if ! is_port_in_use "${candidate}"; then
      export PORT="${candidate}"
      break
    fi
  done
fi

if is_port_in_use "${PORT}"; then
  echo "Port ${PORT} is already in use."
  echo "Try: PORT=3001 ./start_server.sh"
  exit 1
fi

echo "Starting server on http://localhost:${PORT}"
exec python3 server.py

