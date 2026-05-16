#!/bin/bash
#
# cross-mode.sh — verify that objects written in one storage mode are
# readable after the server is restarted in the other mode. This was a real
# bug at one point (fileStorage.Open hard-rejected pack refs); the regression
# test keeps it from coming back.
#
# Three phases against the same data_dir:
#   1. write 21 packable objects + 1 standalone-large in CONCAT mode
#   2. restart in FILE mode, read everything back (including pack refs)
#      and write a new file-mode object
#   3. restart in CONCAT mode, read both the legacy pack ref and the
#      file-mode object written in phase 2 (plus a range read)
#
# Environment:
#   SMOLS3_BIN          path to smols3 binary (default: ./smols3 in repo root)
#   SMOLS3_PORT         listen port (default 9000)
#
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${SMOLS3_BIN:-$REPO_ROOT/smols3}"
PORT="${SMOLS3_PORT:-9000}"

if [ ! -x "$BIN" ]; then
    echo "smols3 binary not found at $BIN" >&2
    echo "build it first: (cd $REPO_ROOT && go build -o smols3 ./cmd/smols3)" >&2
    exit 1
fi

for pid in $(ps -e -o pid,cmd | awk -v b="$BIN" '$2==b {print $1}'); do
    kill -9 "$pid" 2>/dev/null || true
done
sleep 1

DATA=$(mktemp -d /tmp/smols3-xmode-XXXXXX)
LOG="$DATA/server.log"
trap 'rm -rf "$DATA"' EXIT

export SMOLS3_ENDPOINT="http://127.0.0.1:$PORT"

start_server() {
    local mode=$1
    "$BIN" serve --data-dir "$DATA" --listen "127.0.0.1:$PORT" --mode "$mode" \
        >/dev/null 2>>"$LOG" &
    SVR=$!
    sleep 1
    if ! kill -0 "$SVR" 2>/dev/null; then
        echo "server failed to start (mode=$mode); log:" >&2
        cat "$LOG" >&2
        exit 1
    fi
}

stop_server() {
    kill "$SVR" 2>/dev/null || true
    wait "$SVR" 2>/dev/null
}

echo "==== Phase 1: write objects in CONCAT mode ===="
start_server concat

python3 <<'PY'
import os
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url=os.environ["SMOLS3_ENDPOINT"],
    aws_access_key_id="smols3", aws_secret_access_key="smols3secret",
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
s3.create_bucket(Bucket="test")
s3.put_object(Bucket="test", Key="small.txt", Body=b"hihihihihih")
for i in range(20):
    s3.put_object(Bucket="test", Key=f"obj{i}", Body=f"obj {i}\n".encode())
# Large object (> max_packable_object_size default 1 MiB) -> standalone file
s3.put_object(Bucket="test", Key="big.txt", Body=b"x" * (2 * 1024 * 1024))
print("phase1 ok: wrote 22 objects (21 packed, 1 standalone)")
PY
P1_RC=$?
stop_server

echo ""
echo "==== Disk state after phase 1 ===="
find "$DATA/test" -type f -printf '%P  (%s bytes)\n' | sort
echo ""

[ $P1_RC -eq 0 ] || { echo "FAIL: phase 1 did not complete"; exit 1; }

echo "==== Phase 2: restart in FILE mode, read pack-mode objects ===="
start_server file

python3 <<'PY'
import os, sys
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url=os.environ["SMOLS3_ENDPOINT"],
    aws_access_key_id="smols3", aws_secret_access_key="smols3secret",
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))

failures = 0

body = s3.get_object(Bucket="test", Key="small.txt")["Body"].read()
if body != b"hihihihihih":
    print(f"FAIL small.txt: got {body!r}"); failures += 1
else:
    print("ok: small.txt (pack ref) readable in file mode")

mism = 0
for i in range(20):
    body = s3.get_object(Bucket="test", Key=f"obj{i}")["Body"].read()
    want = f"obj {i}\n".encode()
    if body != want:
        mism += 1
if mism == 0:
    print("ok: 20 packed objN all read back correctly")
else:
    print(f"FAIL: {mism} of 20 packed objects mismatched"); failures += 1

body = s3.get_object(Bucket="test", Key="big.txt")["Body"].read()
if body != b"x" * (2 * 1024 * 1024):
    print("FAIL big.txt body mismatch"); failures += 1
else:
    print("ok: big.txt (file ref) readable in file mode")

s3.put_object(Bucket="test", Key="new-in-file-mode.txt", Body=b"written in file mode")
body = s3.get_object(Bucket="test", Key="new-in-file-mode.txt")["Body"].read()
if body != b"written in file mode":
    print("FAIL new-in-file-mode read"); failures += 1
else:
    print("ok: new object written in file mode round-trips")

if failures:
    sys.exit(1)
PY
P2_RC=$?
stop_server

[ $P2_RC -eq 0 ] || { echo "FAIL: phase 2 reads failed"; exit 1; }

echo ""
echo "==== Phase 3: restart in CONCAT mode, read file-mode object ===="
start_server concat

python3 <<'PY'
import os, sys
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url=os.environ["SMOLS3_ENDPOINT"],
    aws_access_key_id="smols3", aws_secret_access_key="smols3secret",
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))

body = s3.get_object(Bucket="test", Key="new-in-file-mode.txt")["Body"].read()
assert body == b"written in file mode", f"got {body!r}"
print("ok: file-mode object readable in concat mode")

body = s3.get_object(Bucket="test", Key="obj5")["Body"].read()
assert body == b"obj 5\n", f"got {body!r}"
print("ok: pack ref still readable")

r = s3.get_object(Bucket="test", Key="big.txt", Range="bytes=0-99")
assert len(r["Body"].read()) == 100
print("ok: range read on cross-mode object")
PY
P3_RC=$?
stop_server

[ $P3_RC -eq 0 ] || { echo "FAIL: phase 3 reads failed"; exit 1; }

echo ""
echo "CROSS-MODE READS: ALL PASS"
