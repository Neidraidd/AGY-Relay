#!/usr/bin/env bash
# AGY Web Proxy — Daemon loop with auto-restart
# Ensures the server stays running continuously and restarts automatically if killed or closed.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export AGY_PROXY_HOST="${AGY_PROXY_HOST:-0.0.0.0}"
export AGY_PROXY_PORT="${AGY_PROXY_PORT:-7788}"
export AGY_WORKSPACE="${AGY_WORKSPACE:-$HOME}"
export AGY_BIN="${AGY_BIN:-agy}"

# Ignore SIGHUP signal so parent terminal disconnects don't terminate the process
trap '' HUP

echo "[AGY Proxy Daemon] Starting daemon loop for AGY Proxy on port $AGY_PROXY_PORT..."

while true; do
    python3 server.py
    EXIT_CODE=$?
    echo "[AGY Proxy Daemon] Server exited with code $EXIT_CODE. Restarting in 2 seconds..."
    sleep 2
done
