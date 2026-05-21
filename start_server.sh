#!/usr/bin/env bash
set -uo pipefail

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

echo "Starting server on http://localhost:${PORT} (auto-reload enabled)"

# Cleanup background process on exit
trap 'echo "[watcher] Shutting down..."; kill "$SERVER_PID" 2>/dev/null; exit' SIGINT SIGTERM

get_checksum() {
  md5 -q "$ROOT_DIR/server.py" 2>/dev/null || md5sum "$ROOT_DIR/server.py" | awk '{print $1}'
}

LAST_CHECKSUM=$(get_checksum)

while true; do
  python3 server.py &
  SERVER_PID=$!

  echo "[watcher] Server started (PID $SERVER_PID)"

  while kill -0 "$SERVER_PID" 2>/dev/null; do
    sleep 1
    CURRENT_CHECKSUM=$(get_checksum)
    if [[ "$CURRENT_CHECKSUM" != "$LAST_CHECKSUM" ]]; then
      echo "[watcher] server.py changed — restarting..."
      LAST_CHECKSUM=$CURRENT_CHECKSUM
      kill "$SERVER_PID" 2>/dev/null
      wait "$SERVER_PID" 2>/dev/null
      break
    fi
  done

  # If server exited on its own (crash), pause briefly before restarting
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "[watcher] Server exited. Restarting in 1s..."
    sleep 1
  fi
done
