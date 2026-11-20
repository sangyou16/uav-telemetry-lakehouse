"""Copy local landing files to S3.

CLI: python -m uav_lakehouse.upload data/landing s3a://lake/landing

In production the ground receivers would write to S3 directly; here the
generator writes locally and this stands in for that hop.
Uses S3_ENDPOINT_URL and the usual AWS_* environment variables.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def split_uri(uri: str) -> tuple[str, str]:
    """'s3a://bucket/some/prefix' -> ('bucket', 'some/prefix')."""
    scheme, _, rest = uri.partition("://")
    if scheme not in ("s3", "s3a") or not rest:
        raise ValueError(f"expected s3a://bucket/prefix, got {uri!r}")
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix.strip("/")


def upload_dir(local_dir: Path, uri: str) -> int:
    import boto3  # optional dependency: pip install -e ".[s3]"

    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL"))
    bucket, prefix = split_uri(uri)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    n = 0
    for path in sorted(Path(local_dir).rglob("*")):
        if path.is_file():
            key = "/".join(p for p in (prefix, path.relative_to(local_dir).as_posix()) if p)
            s3.upload_file(str(path), bucket, key)
            n += 1
    return n


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Upload landing files to S3.")
    p.add_argument("local_dir", type=Path)
    p.add_argument("uri", help="s3a://bucket/prefix")
    args = p.parse_args(argv)
    print(f"uploaded {upload_dir(args.local_dir, args.uri)} files to {args.uri}")


if __name__ == "__main__":
    main()
