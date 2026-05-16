#!/usr/bin/env python3
"""End-to-end smoke test for smols3 using boto3.

Drives a running server through the full S3 client surface we care about:
buckets, single PUT/GET/HEAD/DELETE, user metadata, range requests, prefix
+ delimiter listing, pagination, multipart upload (verifies the -N ETag),
CopyObject, and batch DeleteObjects.

Configuration via environment:

    SMOLS3_ENDPOINT     default http://127.0.0.1:9000
    SMOLS3_ACCESS_KEY   default smols3
    SMOLS3_SECRET_KEY   default smols3secret
    SMOLS3_REGION       default us-east-1
    SMOLS3_BUCKET       default smoke-test    (created and torn down)

Exit code 0 on success, non-zero on first failure.
"""
import hashlib
import io
import os
import sys

import boto3
from botocore.client import Config

ENDPOINT = os.environ.get("SMOLS3_ENDPOINT", "http://127.0.0.1:9000")
ACCESS = os.environ.get("SMOLS3_ACCESS_KEY", "smols3")
SECRET = os.environ.get("SMOLS3_SECRET_KEY", "smols3secret")
REGION = os.environ.get("SMOLS3_REGION", "us-east-1")
BUCKET = os.environ.get("SMOLS3_BUCKET", "smoke-test")


def client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def assert_eq(a, b, msg):
    if a != b:
        print(f"FAIL {msg}: expected {b!r}, got {a!r}", file=sys.stderr)
        sys.exit(1)
    print(f"  ok: {msg}")


def main():
    s3 = client()

    # 1. Create bucket
    s3.create_bucket(Bucket=BUCKET)
    print(f"created bucket '{BUCKET}'")

    # 2. List buckets
    lb = s3.list_buckets()
    names = [b["Name"] for b in lb["Buckets"]]
    assert_eq(BUCKET in names, True, "list_buckets contains target")

    # 3. Head bucket
    s3.head_bucket(Bucket=BUCKET)
    print("  head_bucket ok")

    # 4. Put a small object
    body_small = b"hello world\n" * 10
    md5_small = hashlib.md5(body_small).hexdigest()
    r = s3.put_object(Bucket=BUCKET, Key="small.txt", Body=body_small, ContentType="text/plain")
    etag = r["ETag"].strip('"')
    assert_eq(etag, md5_small, "small put ETag matches MD5")

    # 5. Get it back
    got = s3.get_object(Bucket=BUCKET, Key="small.txt")
    body = got["Body"].read()
    assert_eq(body, body_small, "small get body roundtrip")
    assert_eq(got["ContentType"], "text/plain", "small get content-type")

    # 6. Head
    h = s3.head_object(Bucket=BUCKET, Key="small.txt")
    assert_eq(h["ContentLength"], len(body_small), "head content-length")

    # 7. Range request
    rng = s3.get_object(Bucket=BUCKET, Key="small.txt", Range="bytes=0-4")
    assert_eq(rng["Body"].read(), b"hello", "range get 0-4")

    # 8. User metadata (boto3 may title-case dict keys; compare case-insensitive)
    s3.put_object(Bucket=BUCKET, Key="meta.txt", Body=b"x", Metadata={"foo": "bar"})
    h2 = s3.head_object(Bucket=BUCKET, Key="meta.txt")
    meta_ci = {k.lower(): v for k, v in (h2.get("Metadata") or {}).items()}
    assert_eq(meta_ci.get("foo"), "bar", "user metadata roundtrip (ci)")

    # 9. List objects
    lst = s3.list_objects_v2(Bucket=BUCKET)
    keys = sorted([o["Key"] for o in lst["Contents"]])
    assert_eq(keys, ["meta.txt", "small.txt"], "list_objects_v2 keys")

    # 10. Prefix + delimiter
    for k in ("dir1/a", "dir1/b", "dir2/c", "top"):
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
    pd = s3.list_objects_v2(Bucket=BUCKET, Prefix="", Delimiter="/")
    cps = sorted([p["Prefix"] for p in pd.get("CommonPrefixes", [])])
    keys_top = sorted([o["Key"] for o in pd.get("Contents", [])])
    assert_eq(cps, ["dir1/", "dir2/"], "common prefixes")
    assert_eq("top" in keys_top, True, "top-level key present")
    assert_eq("meta.txt" in keys_top, True, "meta.txt present at top level")

    # 11. Pagination
    for i in range(7):
        s3.put_object(Bucket=BUCKET, Key=f"page/{i:02d}", Body=b"x")
    page1 = s3.list_objects_v2(Bucket=BUCKET, Prefix="page/", MaxKeys=3)
    assert_eq(page1["KeyCount"], 3, "page1 keycount")
    assert_eq(page1["IsTruncated"], True, "page1 truncated")
    page2 = s3.list_objects_v2(
        Bucket=BUCKET, Prefix="page/", MaxKeys=3,
        ContinuationToken=page1["NextContinuationToken"],
    )
    assert_eq(page2["KeyCount"], 3, "page2 keycount")
    page3 = s3.list_objects_v2(
        Bucket=BUCKET, Prefix="page/", MaxKeys=3,
        ContinuationToken=page2["NextContinuationToken"],
    )
    assert_eq(page3["KeyCount"], 1, "page3 final keycount")
    assert_eq(page3["IsTruncated"], False, "page3 not truncated")

    # 12. Multipart upload (16 MiB triggers multipart with default boto3 chunk)
    big_body = os.urandom(16 * 1024 * 1024)
    big_md5 = hashlib.md5(big_body).hexdigest()
    s3.upload_fileobj(
        io.BytesIO(big_body), BUCKET, "big.bin",
        Config=boto3.s3.transfer.TransferConfig(
            multipart_threshold=5 * 1024 * 1024,
            multipart_chunksize=5 * 1024 * 1024,
        ),
    )
    got_big = s3.get_object(Bucket=BUCKET, Key="big.bin")
    body = got_big["Body"].read()
    assert_eq(hashlib.md5(body).hexdigest(), big_md5, "big object roundtrip")

    # ETag should be multipart form (ends with -N)
    head_big = s3.head_object(Bucket=BUCKET, Key="big.bin")
    etag = head_big["ETag"].strip('"')
    assert_eq("-" in etag, True, f"big ETag is multipart form: {etag}")

    # 13. Copy object
    s3.copy_object(Bucket=BUCKET, Key="small-copy.txt",
                   CopySource={"Bucket": BUCKET, "Key": "small.txt"})
    got_copy = s3.get_object(Bucket=BUCKET, Key="small-copy.txt")
    assert_eq(got_copy["Body"].read(), body_small, "copy roundtrip")

    # 14. Delete single
    s3.delete_object(Bucket=BUCKET, Key="small.txt")
    try:
        s3.head_object(Bucket=BUCKET, Key="small.txt")
        print("FAIL: small.txt should be gone", file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("  small.txt gone ok")

    # 15. Delete batch
    s3.delete_objects(
        Bucket=BUCKET,
        Delete={"Objects": [{"Key": f"page/{i:02d}"} for i in range(7)]},
    )
    lst = s3.list_objects_v2(Bucket=BUCKET, Prefix="page/")
    assert_eq(lst.get("KeyCount", 0), 0, "batch delete cleared page/")

    # 16. Cleanup: empty bucket and delete
    leftover = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    if leftover:
        s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": o["Key"]} for o in leftover]},
        )
    s3.delete_bucket(Bucket=BUCKET)
    print(f"deleted bucket '{BUCKET}'")

    print("\nALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    main()
