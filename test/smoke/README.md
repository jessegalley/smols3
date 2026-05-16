# Integration tests

End-to-end tests that drive a running `smols3` instance with the boto3 AWS
SDK. Each script boots a fresh server in a temp data directory, runs the
scenario, kills the server, and reports.

## Dependencies

- `python3` with `boto3` (and `botocore`):
  ```bash
  pip install --user boto3
  ```
- A built `smols3` binary at `./bin/smols3`:
  ```bash
  make
  ```

Override the binary path with `SMOLS3_BIN=/path/to/smols3` and the listen
port with `SMOLS3_PORT=9999` if needed.

## Scripts

### `run-smoke.sh [file|concat] [--debug]`

Boots a server in the given storage mode and runs `smoke.py` against it.
~16 scenarios:

- bucket lifecycle (create / head / list / delete)
- PutObject / GetObject / HeadObject (with `Content-Type` and user metadata)
- Range request
- DeleteObject (single + batch)
- ListObjectsV2 with prefix + delimiter, including CommonPrefixes
- pagination across 3 pages of `MaxKeys=3`
- multipart upload (16 MiB, verifies the `-N` ETag suffix)
- CopyObject

Run both modes:

```bash
./test/smoke/run-smoke.sh file
./test/smoke/run-smoke.sh concat
```

### `cross-mode.sh`

Regression test for the cross-mode read bug: storage mode dictates only how
new PUTs are placed; reads must work regardless. Three phases against the
same data directory:

1. write 21 packable objects + 1 standalone-large in concat mode
2. restart in file mode; read everything back, then write a new file-mode object
3. restart in concat mode; read the file-mode object plus a pack ref, plus a range read

```bash
./test/smoke/cross-mode.sh
```

### `../awscli.sh`

Convenience env / aliases for poking at a running server interactively with
the AWS CLI. Source it: `. ./test/awscli.sh`.

## smoke.py — standalone use

Point it at any running endpoint via env vars:

```bash
SMOLS3_ENDPOINT=http://127.0.0.1:9000 \
SMOLS3_ACCESS_KEY=smols3 \
SMOLS3_SECRET_KEY=smols3secret \
python3 ./test/smoke/smoke.py
```

Useful for testing against a server you started by hand, or against a real
S3 endpoint as a sanity check on the suite itself.
