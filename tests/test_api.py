"""End-to-end HTTP tests: full API contract, cancel and job isolation."""
import io
import threading
import time

import pytest

from app.csvutil import render_subset_csv
from app.jobs import JobManager
from app.main import app
from conftest import requires_signals
from fastapi.testclient import TestClient

SQL_DIVZERO = (
    "SELECT item, SUM(1 // CAST(value AS INTEGER)) "
    "FROM readings GROUP BY item"
)
SQL_OK = "SELECT count(*) AS n FROM readings"
SLOW_SQL = """
WITH r AS (SELECT range AS i FROM range(1200))
SELECT count(*) FROM r a, r b, r c, readings
"""


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # Isolated work directory per test; patch the name inside the main module
    # so route handlers (which reference the module global) pick it up.
    fresh = JobManager(str(tmp_path / "jobs"))
    monkeypatch.setattr("app.main.manager", fresh)
    return TestClient(app)


def _upload(client, rows, sql):
    buf = io.BytesIO()
    buf.write(render_subset_csv(rows).encode("utf-8"))
    buf.seek(0)
    return client.post(
        "/api/jobs",
        files={"file": ("data.csv", buf, "text/csv")},
        data={"sql": sql},
    )


def _wait(client, job_id, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/api/jobs/{job_id}")
        body = r.json()
        if body["status"] in {
            "succeeded", "no_error", "timed_out", "failed", "canceled"
        }:
            return body
        time.sleep(0.1)
    raise AssertionError("job never finished")


def test_full_flow_minimizes_and_download_preserves_shape(client):
    rows = [["s%d" % i, "m", "5"] for i in range(199)]
    rows.append(["s199", "m", "0"])
    r = _upload(client, rows, SQL_DIVZERO)
    assert r.status_code == 200, r.text
    job_id = r.json()["id"]

    final = _wait(client, job_id)
    assert final["status"] == "succeeded"
    assert final["result_rows"] == 1
    assert final["error_type"] == "InvalidInputException"
    assert "Division by zero" in final["first_line"]

    dl = client.get(f"/api/jobs/{job_id}/download")
    assert dl.status_code == 200
    text = dl.text
    assert text.splitlines()[0] == "sample_id,item,value"
    body_lines = [ln for ln in text.splitlines()[1:] if ln.strip()]
    assert len(body_lines) == 1
    assert body_lines[0].endswith(",m,0")

    # Artifact on disk keeps the original header.
    import csv as csvmod
    parsed = list(csvmod.reader(io.StringIO(text)))
    assert parsed[0] == ["sample_id", "item", "value"]


def test_original_query_success_means_no_reduction(client):
    rows = [["a", "x", "1"], ["b", "x", "2"]]
    r = _upload(client, rows, SQL_OK)
    final = _wait(client, r.json()["id"])
    assert final["status"] == "no_error"
    # No result may be published.
    dl = client.get(f"/api/jobs/{r.json()['id']}/download")
    assert dl.status_code == 409


def test_timeout_job_publishes_nothing(client):
    rows = [["a", "x", "1"]]
    r = _upload(client, rows, SLOW_SQL)
    job_id = r.json()["id"]
    # In signal-capable environments the kill is immediate; in restricted
    # sandboxes we wait for the ~2.4 s in-flight query to finish, but the job
    # is still classified strictly as a timeout and publishes nothing.
    final = _wait(client, job_id, timeout=30.0)
    assert final["status"] == "timed_out"
    assert final["has_result"] is False
    dl = client.get(f"/api/jobs/{job_id}/download")
    assert dl.status_code == 409


@requires_signals
def test_cancel_before_run_reports_canceled_with_no_result(client):
    # A slow query guarantees the job is still running when we cancel.
    rows = [["a", "x", "1"]]
    r = _upload(client, rows, SLOW_SQL)
    job_id = r.json()["id"]
    time.sleep(0.4)
    ack = client.post(f"/api/jobs/{job_id}/cancel")
    assert ack.status_code == 200
    assert ack.json()["acknowledged"] is True

    final = _wait(client, job_id, timeout=15.0)
    assert final["status"] == "canceled"
    assert final["has_result"] is False
    dl = client.get(f"/api/jobs/{job_id}/download")
    assert dl.status_code == 409


@requires_signals
def test_cancel_only_acknowledged_after_trials_stop(client):
    rows = [["a", "x", "1"]]
    r = _upload(client, rows, SLOW_SQL)
    job_id = r.json()["id"]
    time.sleep(0.4)
    client.post(f"/api/jobs/{job_id}/cancel")
    # The terminal "canceled" state must not appear while a trial is running.
    seen_canceling = False
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["status"] == "canceling":
            seen_canceling = True
        if body["status"] == "canceled":
            break
        time.sleep(0.05)
    assert seen_canceling or body["status"] == "canceled"
    assert body["status"] == "canceled"


@requires_signals
def test_two_jobs_run_and_cancel_independently(client):
    # Job A: slow (will be canceled). Job B: fast reducible error.
    slow = _upload(client, [["a", "x", "1"]], SLOW_SQL).json()["id"]
    fast_rows = [["f%d" % i, "m", "5"] for i in range(9)] + [["f9", "m", "0"]]
    fast = _upload(client, fast_rows, SQL_DIVZERO).json()["id"]
    assert slow != fast

    time.sleep(0.4)
    client.post(f"/api/jobs/{slow}/cancel")
    slow_final = _wait(client, slow, timeout=15.0)
    fast_final = _wait(client, fast, timeout=30.0)

    assert slow_final["status"] == "canceled"
    assert fast_final["status"] == "succeeded"
    assert fast_final["result_rows"] == 1
    # The canceled job never published; the other job's download works.
    assert client.get(f"/api/jobs/{slow}/download").status_code == 409
    assert client.get(f"/api/jobs/{fast}/download").status_code == 200


def test_validation_error_returns_400(client):
    r = client.post(
        "/api/jobs",
        files={"file": ("x.csv", b"wrong,header\n1,2\n", "text/csv")},
        data={"sql": "SELECT 1"},
    )
    assert r.status_code == 400
    assert "表头" in r.json()["detail"]


def test_unknown_job_404(client):
    assert client.get("/api/jobs/nope").status_code == 404
    assert client.post("/api/jobs/nope/cancel").status_code == 404


def test_demo_endpoints(client):
    info = client.get("/api/demo")
    assert info.status_code == 200
    demos = {d["id"]: d for d in info.json()["demos"]}
    assert "agg_divzero" in demos
    csv_resp = client.get("/api/demo/agg_divzero/csv")
    assert csv_resp.status_code == 200
    lines = [ln for ln in csv_resp.text.splitlines() if ln.strip()]
    assert len(lines) == 1001  # header + 1000 rows
    assert lines[0] == "sample_id,item,value"


def test_demo_agg_divzero_end_to_end(client):
    # The shipped "aggregate-only" demo must minimize to a single row.
    info = client.get("/api/demo").json()["demos"]
    demo = next(d for d in info if d["id"] == "agg_divzero")
    csv_resp = client.get("/api/demo/agg_divzero/csv")
    r = client.post(
        "/api/jobs",
        files={"file": ("agg_divzero.csv", csv_resp.content, "text/csv")},
        data={"sql": demo["sql"]},
    )
    final = _wait(client, r.json()["id"], timeout=60.0)
    assert final["status"] == "succeeded"
    assert final["result_rows"] == demo["expected_min_rows"] == 1


def test_sse_stream_reaches_terminal(client):
    rows = [["a", "x", "1"], ["b", "x", "0"]]
    r = _upload(client, rows, SQL_DIVZERO)
    job_id = r.json()["id"]
    events = []
    with client.stream("GET", f"/api/jobs/{job_id}/events") as resp:
        for line in resp.iter_lines():
            if not line:
                continue
            events.append(line)
            if '"terminal"' in line:
                break
            if len(events) > 200:
                break
    assert any('"terminal"' in e for e in events)


def test_cancel_is_independent_and_publishes_nothing_in_sandbox(client):
    """Runs even where signals are denied.

    A fast reducible job is canceled after submission; the manager must still
    reach the ``canceled`` terminal state, never publish a result, and a second
    concurrent job must be completely unaffected.
    """
    from app.jobs import JobManager as _JM  # noqa: F401

    # Job B (unaffected): a normal fast minimization that should succeed.
    fast_rows = [["g%d" % i, "m", "5"] for i in range(9)] + [["g9", "m", "0"]]
    fast_id = _upload(client, fast_rows, SQL_DIVZERO).json()["id"]

    # Job A: many rows so it performs several trials; cancel right away.
    slow_rows = [["a%d" % i, "m", ("0" if i == 50 else "5")] for i in range(60)]
    cancel_id = _upload(client, slow_rows, SQL_DIVZERO).json()["id"]
    client.post(f"/api/jobs/{cancel_id}/cancel")

    canceled = _wait(client, cancel_id, timeout=30.0)
    assert canceled["status"] == "canceled"
    assert canceled["has_result"] is False
    assert client.get(f"/api/jobs/{cancel_id}/download").status_code == 409

    fast = _wait(client, fast_id, timeout=30.0)
    assert fast["status"] == "succeeded"
    assert client.get(f"/api/jobs/{fast_id}/download").status_code == 200
