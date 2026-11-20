#!/usr/bin/env bash
# Run the whole pipeline on a local S3-compatible server (moto), end to end.
#   pip install -e ".[dev,spark,s3]"
#   bash scripts/run_on_s3.sh
# First run downloads the ~650 MB AWS SDK that Spark's s3a connector needs.
set -euo pipefail

PORT="${PORT:-9000}"
export S3_ENDPOINT_URL="http://127.0.0.1:${PORT}"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
LAKE="s3a://lake/lake"
LANDING="s3a://lake/landing"

moto_server -H 127.0.0.1 -p "${PORT}" > /tmp/moto_server.log 2>&1 &
MOTO_PID=$!
trap 'kill ${MOTO_PID}' EXIT  # stop the S3 server however the script ends
until curl -s -o /dev/null "${S3_ENDPOINT_URL}"; do sleep 0.5; done
echo "local S3 server running at ${S3_ENDPOINT_URL}"

python -m uav_lakehouse.generator --out data/landing
python -m uav_lakehouse.upload data/landing "${LANDING}"
python -m uav_lakehouse.bronze --landing "${LANDING}" --lake "${LAKE}"
python -m uav_lakehouse.silver --lake "${LAKE}"
python -m uav_lakehouse.gold --lake "${LAKE}"

echo
echo "objects in the bucket, by table:"
python - <<'PY'
import collections, os, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
counts = collections.Counter()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket="lake"):
    for obj in page.get("Contents", []):
        parts = obj["Key"].split("/")
        counts["/".join(parts[:3] if parts[0] == "lake" else parts[:2])] += 1
for prefix, n in sorted(counts.items()):
    print(f"  s3://lake/{prefix:<32} {n:>4} objects")
PY
