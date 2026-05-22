#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

is_port_in_use() {
  local port="$1"
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
}

port_pids() {
  local port="$1"
  lsof -nP -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true
}

owns_repo_server() {
  local pid="$1"
  local command
  local cwd
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -n 1)"
  [[ "$command" == *"server.py"* && "$cwd" == "$ROOT_DIR" ]]
}

reclaim_port() {
  local port="$1"
  local pid
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if owns_repo_server "$pid"; then
      echo "[watcher] Reclaiming port ${port} from existing stock_analysis server PID ${pid}"
      kill "$pid" 2>/dev/null || true
    else
      echo "Port ${port} is already in use by another process (PID ${pid})."
      echo "Not killing it because it is not this repo's server.py."
      echo "Stop that process, then run ./start_server.sh again."
      exit 1
    fi
  done < <(port_pids "$port")

  sleep 1
}

export PORT=3000
reclaim_port "$PORT"

if is_port_in_use "${PORT}"; then
  echo "Port ${PORT} is already in use."
  echo "Try: lsof -nP -iTCP:${PORT} -sTCP:LISTEN"
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
