"""End to end on S3: the real CLIs against a throwaway local S3 server (moto).

Each layer runs in its own process, exactly as a user or scheduler would run it.
Slow on the first run: Spark downloads the ~650 MB AWS SDK once.
"""

import json
import os
import re
import socket
import subprocess
import sys

import pytest

moto_server = pytest.importorskip("moto.server", reason='needs pip install -e ".[s3]"')

from uav_lakehouse.generator.config import SimConfig  # noqa: E402
from uav_lakehouse.generator.simulate import simulate  # noqa: E402
from uav_lakehouse.generator.writer import write_run  # noqa: E402

pytestmark = pytest.mark.s3

CFG = SimConfig(n_aircraft=2, duration_s=60, seed=3)
LANDING = "s3a://lake/landing"
LAKE = "s3a://lake/lake"


@pytest.fixture(scope="module")
def s3_env():
    with socket.socket() as s:  # pick a free port
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    env = {
        **os.environ,
        "S3_ENDPOINT_URL": f"http://127.0.0.1:{port}",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    yield env
    server.stop()


def run(env, *args):
    out = subprocess.run(
        [sys.executable, "-m", *args], env=env, capture_output=True, text=True, timeout=900
    )
    assert out.returncode == 0, out.stderr[-3000:]
    return out.stdout


def number(pattern, text):
    return int(re.search(pattern, text).group(1).replace(",", ""))


def test_pipeline_on_s3_matches_ground_truth(s3_env, tmp_path):
    run_dir = write_run(simulate(CFG), CFG, tmp_path / "landing", "run-a")
    totals = json.loads((run_dir / "_manifest.json").read_text())["totals"]

    run(s3_env, "uav_lakehouse.upload", str(tmp_path / "landing"), LANDING)
    bronze = run(s3_env, "uav_lakehouse.bronze", "--landing", LANDING, "--lake", LAKE)
    silver = run(s3_env, "uav_lakehouse.silver", "--lake", LAKE)
    gold = run(s3_env, "uav_lakehouse.gold", "--lake", LAKE)

    lines = sum(len(p.read_text().splitlines()) for p in run_dir.glob("batch-*.jsonl"))
    assert number(r"appended ([\d,]+) rows", bronze) == lines
    assert number(r"telemetry\s+inserted\s+([\d,]+)", silver) == (
        totals["sent"] - totals["missing_seqs"]
    )
    assert number(r"quarantine\s+inserted\s+([\d,]+)", silver) == totals["corrupted_lines"]
    assert number(r"stream_health: (\d+) streams", gold) == 2 * len(CFG.messages)

    # Re-running bronze on S3 is still idempotent (checkpoint lives in S3 too).
    again = run(s3_env, "uav_lakehouse.bronze", "--landing", LANDING, "--lake", LAKE)
    assert number(r"appended ([\d,]+) rows", again) == 0
