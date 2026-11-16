"""Regression tests for the cancel / publish handoff.

These tests reproduce the maintenance-warehouse incident: after the
maintenance worker approves a cancellation, the (not yet published) reduced
sample must never be handed to a colleague.  The tricky window is the publish
phase -- between the end of minimization and the result file becoming visible.

Determinism
-----------
Real slow disks and ``time.sleep`` make wall-clock tests flaky on loaded CI
machines.  Instead the worker is parked on an explicit *publish gate*: a
per-job ``threading.Event`` supplied through ``JobManager.publish_gate_factory``
that blocks result publishing until the test releases it.  The gate records
its waiter count, so a test can wait until the worker is provably parked and
then exercise every cancel/query interleaving with no timing assumptions.

The environment-variable pause (``SAMPLE_MINIMIZER_PUBLISH_PAUSE_SECONDS``),
which documents the same seam for operators, is covered separately.
"""
import io
import threading
import time

import pytest

from app.csvutil import render_subset_csv
from app.jobs import PUBLISH_PAUSE_ENV, JobManager
from app.main import app
from fastapi.testclient import TestClient

SQL_DIVZERO = (
    "SELECT item, SUM(1 // CAST(value AS INTEGER)) "
    "FROM readings GROUP BY item"
)

# Fast, deterministic reducible input: exactly one zero row -> 1-row sample.
FAST_ROWS = [["g%d" % i, "m", "5"] for i in range(19)] + [["g19", "m", "0"]]


class Gate:
    """An Event-like object that exposes how many threads wait on it.

    Composition (not an ``Event`` subclass): the built-in Event's C-level
    ``wait`` does not dispatch to overridden methods.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._counter_lock = threading.Lock()
        self.waiters = 0

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout=None) -> bool:
        with self._counter_lock:
            self.waiters += 1
        try:
            return self._event.wait(timeout)
        finally:
            with self._counter_lock:
                self.waiters -= 1


@pytest.fixture()
def parked(tmp_path, monkeypatch):
    """A client whose every job blocks on an observable publish gate."""
    monkeypatch.delenv(PUBLISH_PAUSE_ENV, raising=False)
    gates: dict[str, Gate] = {}

    def publish_gate_factory(job):
        gate = Gate()
        gates[job.id] = gate
        return gate

    manager = JobManager(
        str(tmp_path / "jobs"), publish_gate_factory=publish_gate_factory
    )
    monkeypatch.setattr("app.main.manager", manager)
    return TestClient(app), gates


def _upload(client, rows, sql=SQL_DIVZERO):
    buf = io.BytesIO()
    buf.write(render_subset_csv(rows).encode("utf-8"))
    buf.seek(0)
    return client.post(
        "/api/jobs",
        files={"file": ("data.csv", buf, "text/csv")},
        data={"sql": sql},
    )


def _wait(client, job_id, timeout=30.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        last = body
        if body["status"] in {
            "succeeded", "no_error", "timed_out", "failed", "canceled"
        }:
            return body
        time.sleep(0.02)
    raise AssertionError(f"job never finished: {last}")


def _wait_phase(client, job_id, phase, timeout=20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["progress"]["phase"] == phase:
            return body
        if body["status"] in {
            "succeeded", "no_error", "timed_out", "failed", "canceled"
        }:
            raise AssertionError(
                f"job left before phase {phase}: {body['status']}"
            )
        time.sleep(0.02)
    raise AssertionError(f"never reached phase {phase}")


def _wait_parked(client, gates, job_id):
    """Block until the worker is provably waiting on the publish gate.

    At that point minimization (and every trial) is fully done, phase is
    "done", and no result has been published.
    """
    _wait_phase(client, job_id, "done")
    gate = gates[job_id]
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if gate.waiters > 0:
            break
        time.sleep(0.01)
    assert gate.waiters > 0, "worker never parked on the publish gate"
    body = client.get(f"/api/jobs/{job_id}").json()
    assert body["status"] == "running"
    assert body["has_result"] is False
    assert body["result_rows"] == 0
    return body


def test_cancel_while_parked_ends_canceled_without_result(parked):
    """Cancel while the result write is pending: discard, never publish."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)

    # The not-yet-published invariant: nothing downloadable.
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 409

    t0 = time.monotonic()
    ack = client.post(f"/api/jobs/{job_id}/cancel")
    # The request must be accepted immediately, without waiting on the gate.
    assert time.monotonic() - t0 < 1.0
    assert ack.status_code == 200
    assert ack.json()["status"] == "canceling"

    final = _wait(client, job_id)
    assert final["status"] == "canceled"
    assert final["has_result"] is False
    assert final["result_rows"] == 0
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 409


def test_poll_and_other_window_stay_responsive_while_parked(parked):
    """A blocked publish must not stall queries or another window's job."""
    client, gates = parked
    paused_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, paused_id)

    # Second job from another browser window. Release only its gate, so it
    # runs to completion while the first job stays deterministically parked.
    other_id = _upload(client, FAST_ROWS).json()["id"]

    latencies = []
    saw_other_running = False
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        listing = client.get("/api/jobs")
        latencies.append(time.monotonic() - t0)
        assert listing.status_code == 200
        assert len(listing.json()["jobs"]) == 2
        paused = client.get(f"/api/jobs/{paused_id}").json()
        assert paused["status"] == "running"
        assert paused["has_result"] is False
        other = client.get(f"/api/jobs/{other_id}").json()
        if other["status"] in ("running", "canceling", "queued"):
            saw_other_running = True
        time.sleep(0.05)
        # Release the second job as soon as its gate exists.
        if other_id in gates:
            gates[other_id].set()
    assert saw_other_running
    assert max(latencies) < 1.0

    # The parked job is still blocked (no timing involved): cancel wins.
    assert gates[paused_id].waiters > 0
    client.post(f"/api/jobs/{paused_id}/cancel")
    final = _wait(client, paused_id)
    assert final["status"] == "canceled"
    assert final["has_result"] is False
    assert client.get(f"/api/jobs/{paused_id}/download").status_code == 409

    # The other window's job is completely unaffected and publishes.
    other_final = _wait(client, other_id)
    assert other_final["status"] == "succeeded"
    assert other_final["has_result"] is True
    assert other_final["result_rows"] == 1
    assert client.get(f"/api/jobs/{other_id}/download").status_code == 200


def test_successful_publish_is_atomic_and_immutable(parked, tmp_path):
    """Releasing the gate publishes status / rows / download in one step."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)

    gates[job_id].set()
    final = _wait(client, job_id)
    assert final["status"] == "succeeded"
    assert final["has_result"] is True
    assert final["result_rows"] == 1

    # The job directory contains exactly the audited artifacts and the
    # published sample, never a leftover temporary write.
    job_dir = tmp_path / "jobs" / job_id
    names = sorted(p.name for p in job_dir.iterdir())
    assert names == ["minimal_sample.csv", "original.csv", "query.sql"]

    dl = client.get(f"/api/jobs/{job_id}/download")
    assert dl.status_code == 200
    lines = [ln for ln in dl.text.splitlines() if ln.strip()]
    assert lines[0] == "sample_id,item,value"
    assert len(lines) == 2
    assert lines[1].endswith(",m,0")

    # A published success is immutable: cancel is refused and the result
    # stays available.
    refused = client.post(f"/api/jobs/{job_id}/cancel")
    assert refused.status_code == 409
    assert "不可取消" in refused.json()["detail"]
    again = client.get(f"/api/jobs/{job_id}").json()
    assert again["status"] == "succeeded"
    assert again["has_result"] is True
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 200


def test_canceled_publish_leaves_no_sample_file(parked, tmp_path):
    """The unpublished sample (including the temp write) is discarded."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)
    client.post(f"/api/jobs/{job_id}/cancel")
    final = _wait(client, job_id)
    assert final["status"] == "canceled"

    job_dir = tmp_path / "jobs" / job_id
    names = sorted(p.name for p in job_dir.iterdir())
    # Only the upload audit artifacts remain; no minimal sample, no tmp file.
    assert names == ["original.csv", "query.sql"]


def test_parked_job_never_advertises_a_result(parked):
    """While parked every single snapshot is coherent and unpublished."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)

    for _ in range(20):
        body = client.get(f"/api/jobs/{job_id}").json()
        assert body["status"] == "running"
        assert body["has_result"] is False
        assert body["result_rows"] == 0
        assert client.get(f"/api/jobs/{job_id}/download").status_code == 409
        time.sleep(0.02)

    gates[job_id].set()
    final = _wait(client, job_id)
    assert final["status"] == "succeeded"
    assert final["has_result"] is True
    assert final["result_rows"] == 1
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 200


def test_cancel_ack_never_blocks_on_parked_publish(parked):
    """The cancel endpoint itself stays fast while publishing is blocked."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)
    t0 = time.monotonic()
    ack = client.post(f"/api/jobs/{job_id}/cancel")
    elapsed = time.monotonic() - t0
    assert ack.status_code == 200
    assert ack.json()["acknowledged"] is True
    assert elapsed < 1.0
    assert _wait(client, job_id)["status"] == "canceled"


def test_cancel_request_twice_is_idempotent_and_stays_terminal(parked):
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)
    first = client.post(f"/api/jobs/{job_id}/cancel")
    assert first.status_code == 200
    # Still canceling: a repeated request stays an acknowledgement.
    second = client.post(f"/api/jobs/{job_id}/cancel")
    assert second.status_code == 200
    assert second.json()["status"] == "canceling"
    final = _wait(client, job_id)
    assert final["status"] == "canceled"
    # Once terminal, further cancel attempts are refused, state never flips.
    third = client.post(f"/api/jobs/{job_id}/cancel")
    assert third.status_code == 409
    assert client.get(f"/api/jobs/{job_id}").json()["status"] == "canceled"


def test_monitor_sees_canceling_before_terminal(parked):
    """The observable transition is running -> canceling -> canceled, never
    a jump straight to succeeded with a result."""
    client, gates = parked
    job_id = _upload(client, FAST_ROWS).json()["id"]
    _wait_parked(client, gates, job_id)
    client.post(f"/api/jobs/{job_id}/cancel")

    saw_canceling = False
    deadline = time.monotonic() + 10.0
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/jobs/{job_id}").json()
        if last["status"] == "canceling":
            saw_canceling = True
            assert last["has_result"] is False
        if last["status"] == "canceled":
            break
        time.sleep(0.02)
    assert saw_canceling
    assert last is not None and last["status"] == "canceled"
    assert last["has_result"] is False


def test_env_var_publish_pause_keeps_job_unpublished_then_publishes(
    tmp_path, monkeypatch
):
    """The operator-facing pause knob blocks publishing, then releases it.

    A wall-clock pause alone can be missed by a polling test, so the publish
    gate is used to observe the worker *while* the env pause is still in
    effect; the pause itself is what the worker is sleeping in.
    """
    monkeypatch.setenv(PUBLISH_PAUSE_ENV, "30")  # long enough to never elapse
    gates: dict[str, Gate] = {}
    manager = JobManager(
        str(tmp_path / "jobs"),
        publish_gate_factory=lambda job: gates.setdefault(job.id, Gate()),
    )
    monkeypatch.setattr("app.main.manager", manager)
    client = TestClient(app)

    job_id = _upload(client, FAST_ROWS).json()["id"]
    # The worker is parked inside the env-var pause (the gate only observes;
    # it is never released here).
    _wait_parked(client, gates, job_id)
    body = client.get(f"/api/jobs/{job_id}").json()
    assert body["status"] == "running"
    assert body["has_result"] is False
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 409

    # Clearing the knob and releasing the gate lets publishing complete.
    monkeypatch.delenv(PUBLISH_PAUSE_ENV, raising=False)
    gates[job_id].set()
    final = _wait(client, job_id)
    assert final["status"] == "succeeded"
    assert final["has_result"] is True
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 200
