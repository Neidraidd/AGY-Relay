#!/usr/bin/env bash
# AGY Web Proxy — startup script
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
export AGY_PROXY_HOST="${AGY_PROXY_HOST:-0.0.0.0}"
export AGY_PROXY_PORT="${AGY_PROXY_PORT:-7788}"
export AGY_WORKSPACE="${AGY_WORKSPACE:-$HOME}"
export AGY_BIN="${AGY_BIN:-agy}"

echo "╔══════════════════════════════════════════╗"
echo "║               AGY Relay                  ║"
echo "╠══════════════════════════════════════════╣"
echo "║  URL:       http://localhost:$AGY_PROXY_PORT        ║"
echo "║  Workspace: $AGY_WORKSPACE"
echo "║  AGY bin:   $AGY_BIN"
echo "╚══════════════════════════════════════════╝"
echo ""

# Check agy is available
if ! command -v "$AGY_BIN" &>/dev/null; then
  echo "[!] ERROR: '$AGY_BIN' not found in PATH"
  echo "    Set AGY_BIN=/path/to/agy to override"
  exit 1
fi

# Start server
python3 server.py
