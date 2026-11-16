"""Shared pytest fixtures."""
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.trial import run_trial  # noqa: E402


def _signals_allowed() -> bool:
    """Detect whether this environment permits killing a live child.

    Hardened CI sandboxes (seccomp) can deny EPERM for every signal even to
    one's own children. The Docker deployment target does not have this
    restriction; tests that require an *immediate* kill are skipped there.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True,
    )
    try:
        time.sleep(0.15)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
            return True
        except (PermissionError, ProcessLookupError):
            return False
    finally:
        if proc.poll() is None:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass


SIGNALS_ALLOWED = _signals_allowed()
requires_signals = pytest.mark.skipif(
    not SIGNALS_ALLOWED,
    reason="sandbox denies signals to child processes; covered in Docker",
)


@pytest.fixture
def make_csv():
    import csv
    import io

    def _make(rows, header=("sample_id", "item", "value")):
        buf = io.BytesIO()
        text = io.StringIO()
        w = csv.writer(text, lineterminator="\r\n")
        w.writerow(list(header))
        w.writerows(rows)
        buf.write(text.getvalue().encode("utf-8"))
        buf.seek(0)
        return buf

    return _make


@pytest.fixture
def never_canceled():
    ev = threading.Event()
    assert not ev.is_set()
    return ev


@pytest.fixture
def run_one(never_canceled):
    def _run(rows, sql, canceled_event=None):
        return run_trial(
            [list(r) for r in rows], sql,
            canceled_event or never_canceled,
        )
    return _run


@pytest.fixture
def wait_job():
    """Poll a job id via the API until a terminal state."""
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)

    def _wait(job_id, timeout=60.0):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            r = client.get(f"/api/jobs/{job_id}")
            assert r.status_code == 200
            last = r.json()
            if last["status"] in {
                "succeeded", "no_error", "timed_out", "failed", "canceled"
            }:
                return last
            time.sleep(0.1)
        raise AssertionError(f"job {job_id} did not finish: {last}")

    return _wait, client
