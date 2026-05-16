#!/bin/bash
#
# run-smoke.sh — boot a fresh smols3 instance in a tmpdir and run smoke.py
# against it. Exits non-zero on first failure. Server log is preserved if
# tests fail so you can inspect what happened.
#
# Usage:
#   ./run-smoke.sh                  # default: mode=file
#   ./run-smoke.sh concat           # run in concat mode
#   ./run-smoke.sh file --debug     # bump log level to debug
#
# Environment:
#   SMOLS3_BIN          path to smols3 binary (default: ./bin/smols3 in repo root)
#   SMOLS3_PORT         listen port (default 9000)
#
set -u

MODE="${1:-file}"
shift || true
DEBUG=""
for arg in "$@"; do
    case "$arg" in
        --debug) DEBUG=1 ;;
    esac
done

case "$MODE" in
    file|concat) ;;
    *) echo "usage: $0 [file|concat] [--debug]" >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${SMOLS3_BIN:-$REPO_ROOT/bin/smols3}"
PORT="${SMOLS3_PORT:-9000}"

if [ ! -x "$BIN" ]; then
    echo "smols3 binary not found at $BIN" >&2
    echo "build it first: (cd $REPO_ROOT && make)" >&2
    exit 1
fi

# Kill any previous instances of this exact binary (matches by argv[0]).
for pid in $(ps -e -o pid,cmd | awk -v b="$BIN" '$2==b {print $1}'); do
    kill -9 "$pid" 2>/dev/null || true
done
sleep 1

DATA=$(mktemp -d /tmp/smols3-smoke-XXXXXX)
LOG="$DATA/server.log"
trap 'rm -rf "$DATA"' EXIT

echo "==> mode=$MODE port=$PORT data=$DATA"

FLAGS=(serve --data-dir "$DATA" --listen "127.0.0.1:$PORT" --mode "$MODE")
[ -n "$DEBUG" ] && FLAGS+=(--log-level debug)

"$BIN" "${FLAGS[@]}" >/dev/null 2>"$LOG" &
SVR=$!
sleep 1

if ! kill -0 "$SVR" 2>/dev/null; then
    echo "server failed to start; log:" >&2
    cat "$LOG" >&2
    exit 1
fi

export SMOLS3_ENDPOINT="http://127.0.0.1:$PORT"
timeout 60 python3 "$SCRIPT_DIR/smoke.py"
RC=$?

kill "$SVR" 2>/dev/null || true
wait "$SVR" 2>/dev/null

if [ $RC -ne 0 ]; then
    echo "--- server log (last 40 lines) ---" >&2
    tail -40 "$LOG" >&2
fi
exit $RC
