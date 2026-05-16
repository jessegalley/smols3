# smols3

A single-binary, S3-compatible test server in Go. Persists objects to the local filesystem with two storage modes: one file per object (default) or small-object packing into shared "pack files".

Useful for: local development, CI integration tests, anything that wants to talk to S3 without a real S3 endpoint. Not a production object store — see [Out of scope](#out-of-scope).

---

## Quick start

Zero ceremony — no config file needed:

```bash
make
./bin/smols3 serve
```

That's it. The server:
- Listens on `127.0.0.1:9000`
- Stores data under `$XDG_DATA_HOME/smols3` (falls back to `~/.local/share/smols3`, then `./smols3-data`)
- Authenticates SigV4 with `access_key=smols3` / `secret_key=smols3secret`
- Prints a startup banner showing all of the above

Point any S3 client at `http://127.0.0.1:9000` with those credentials and **path-style addressing**. See `test/smoke/` for end-to-end test scripts with concrete client snippets (aws-cli, boto3).

---

## CLI

| Command | Purpose |
| --- | --- |
| `smols3 serve` | Run the HTTP server. Honors every TOML knob as a flag (`--listen`, `--mode`, `--data-dir`, `--max-concat-size`, etc.). |
| `smols3 init` | Write a `smols3.toml` (default path: `./smols3.toml`) so you can edit it. Optional — `serve` doesn't need it. |
| `smols3 compact` | Offline pack-file compaction. Server must be stopped. |
| `smols3 fsck` | Verify index against on-disk state. `--repair` truncates orphan pack-file tails. |
| `smols3 version` | Print version. |

`./bin/smols3 serve --help` lists every flag.

---

## Configuration

Every setting can be specified three ways. Precedence:

```
defaults  <  config file (--config)  <  CLI flags
```

### Example TOML

```toml
[server]
listen           = "127.0.0.1:9000"
region           = "us-east-1"
read_timeout     = "30s"
write_timeout    = "5m"
max_request_size = "5368709120"  # 5 GiB

[storage]
data_dir                 = "/var/lib/smols3"
index_path               = "/var/lib/smols3/index.db"
mode                     = "file"        # "file" | "concat"
max_object_size          = "5368709120"  # 5 GiB
max_concat_size          = "67108864"    # 64 MiB pack-file cap
max_packable_object_size = "1048576"     # 1 MiB; larger objects go standalone
fsync_data               = true
shard_dir_depth          = 2

[auth]
mode       = "sigv4"        # "sigv4" | "none"
access_key = "smols3"
secret_key = "smols3secret"

[log]
level      = "info"
format     = "text"
access_log = true

[limits]
max_multipart_parts = 10000
min_part_size       = "5242880"  # 5 MiB
```

Byte sizes accept suffixes: `64MiB`, `5GB`, `1024`, etc. Durations use Go syntax: `30s`, `5m`, `2h`.

---

## Storage modes

### `mode = "file"` (default)

Each object → one file. Path under `<data_dir>/<bucket>/<sharded-path>/<hash>_<key-hint>`. Sharding (default 2 hex levels) keeps directory entry counts manageable.

### `mode = "concat"`

Small objects (≤ `max_packable_object_size`, default 1 MiB) are appended into shared **pack files** capped at `max_concat_size` (default 64 MiB). Large objects still get their own file. This avoids the inode/space overhead of millions of tiny files.

Deletions don't reclaim pack-file bytes immediately — run `smols3 compact` (offline) to rewrite live objects into fresh packs and drop the old ones.

The configured mode only affects how *new* PUTs are placed. Reads work regardless: pack-mode objects stay readable after the server is restarted in file mode and vice versa.

---

## Endpoints

What works:

- **Bucket**: `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`, `ListObjectsV2` (prefix, delimiter, pagination), `ListObjectsV1` (legacy clients)
- **Object**: `PutObject`, `GetObject` (with Range), `HeadObject`, `DeleteObject`, `DeleteObjects` (batch), `CopyObject`
- **Multipart**: `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`
- **Tagging**: `Get/Put/DeleteObjectTagging`
- **Auth**: AWS Signature V4 (header form + presigned-URL query form)

Stubbed (returns disabled / canned XML so clients don't choke):

- `GetBucketLocation`, `GetBucketAcl`, `GetBucketVersioning`, `GetBucketCors`, `GetBucketEncryption`

Returns `NotImplemented` (501) for: lifecycle, replication, inventory, analytics, intelligent-tiering, object lock, legal hold, website hosting, accelerate, etc.

---

## Scope

### In scope

- S3 wire compatibility for typical client workflows
- Two on-disk layouts (file vs concat)
- Real SigV4 signature verification
- Multipart uploads with the proper `<md5ofmd5s>-N` ETag
- Path-style addressing

### Out of scope

- Object versioning (returns disabled)
- TLS (run behind a reverse proxy)
- Virtual-hosted-style addressing (`bucket.host:9000`)
- Bucket policies and ACLs (canned `private`)
- Lifecycle, replication, encryption, inventory, analytics, object lock
- Online compaction (compact requires server stopped)
- Multi-tenant credentials (single static pair)

---

## Building

```bash
make           # builds ./bin/smols3
make test      # go test ./...
make vet       # go vet ./...
make clean     # rm -rf bin/
```

Single ~10 MB binary, no CGO, no external runtime dependencies.

---

## Tech

- **Language**: Go 1.22
- **Index**: [bbolt](https://github.com/etcd-io/bbolt) (single-file B+tree, pure Go)
- **HTTP**: net/http + [chi](https://github.com/go-chi/chi) router
- **CLI**: [cobra](https://github.com/spf13/cobra) + [pflag](https://github.com/spf13/pflag)
- **Config**: TOML via [pelletier/go-toml/v2](https://github.com/pelletier/go-toml)

---

## Project layout

```
cmd/smols3/         entrypoint
internal/cli/       cobra subcommands
internal/config/    TOML loader + Defaults
internal/index/     bbolt wrapper, records, listing
internal/storage/   file mode + pack mode (concat)
internal/s3api/     HTTP handlers, XML response shapes
internal/sigv4/     signature verification
internal/etag/      MD5 hashers + multipart ETag combiner
internal/compact/   offline pack compaction
internal/fsck/      consistency check + repair
test/smoke/         end-to-end integration tests (boto3)
```
