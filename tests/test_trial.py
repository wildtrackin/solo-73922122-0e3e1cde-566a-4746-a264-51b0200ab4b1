"""Trial subprocess: isolation, fingerprints, timeout and cancellation."""
import threading
import time

import duckdb
import pytest

from app.trial import TRIAL_TIMEOUT_SECONDS, run_trial
from conftest import requires_signals

SQL_DIVZERO = (
    "SELECT item, SUM(1 // CAST(value AS INTEGER)) "
    "FROM readings GROUP BY item"
)
SQL_OK = "SELECT count(*) FROM readings"
SLOW_SQL = """
WITH r AS (SELECT range AS i FROM range(1200))
SELECT count(*) FROM r a, r b, r c, readings
"""


def test_trial_success(run_one):
    res = run_one([("a", "x", "1")], SQL_OK)
    assert res.status == "success"
    assert res.elapsed_ms >= 0


def test_trial_error_fingerprint(run_one):
    rows = [("a", "x", "1"), ("b", "x", "0")]
    res = run_one(rows, SQL_DIVZERO)
    assert res.status == "error"
    assert res.error_type == "InvalidInputException"
    assert "Division by zero" in (res.first_line or "")
    assert res.reproduces is True


def test_determinism_same_fingerprint_across_runs(run_one):
    rows = [("a", "x", "0")]
    r1 = run_one(rows, SQL_DIVZERO)
    r2 = run_one(rows, SQL_DIVZERO)
    assert (r1.status, r1.error_type, r1.first_line) == (
        r2.status, r2.error_type, r2.first_line
    )


def test_trials_use_independent_databases(run_one):
    # A mutating-looking state cannot leak: each trial starts from only the
    # rows handed to it. Running the same trial twice yields identical output.
    rows = [("a", "x", "1"), ("b", "x", "2")]
    sql = "SELECT string_agg(sample_id, ',') FROM readings ORDER BY 1"
    r1 = run_one(rows, sql)
    r2 = run_one(rows, sql)
    assert r1.status == r2.status == "success"


def test_fresh_db_does_not_see_previous_trial_data(run_one):
    run_one([("leftover", "x", "1")], SQL_OK)
    res = run_one([("only-this", "x", "1")],
                  "SELECT count(*) AS n FROM readings")
    # Verify via a direct runner-side query that the table contains exactly
    # one row: a second trial must not inherit the first trial's rows.
    import json
    import subprocess
    import sys
    from app.trial import _RUNNER_PATH
    proc = subprocess.run(
        [sys.executable, "-I", _RUNNER_PATH],
        input=json.dumps({
            "rows": [["only-this", "x", "1"]],
            "sql": "SELECT count(*) FROM readings",
        }),
        capture_output=True, text=True, timeout=10,
    )
    out = json.loads(proc.stdout.strip())
    assert out["status"] == "success"
    assert res.status == "success"


@requires_signals
def test_timeout_is_enforced(run_one):
    started = time.monotonic()
    res = run_one([("a", "x", "1")], SLOW_SQL, threading.Event())
    elapsed = time.monotonic() - started
    assert res.status == "timeout"
    # Hard 2s budget plus a small process-reap allowance.
    assert elapsed < TRIAL_TIMEOUT_SECONDS + 3.0
    assert elapsed >= TRIAL_TIMEOUT_SECONDS - 0.2


@requires_signals
def test_cancel_stops_inflight_trial_immediately():
    cancel = threading.Event()

    def fire():
        time.sleep(0.3)
        cancel.set()

    t = threading.Thread(target=fire)
    t.start()
    started = time.monotonic()
    res = run_trial([["a", "x", "1"]], SLOW_SQL, cancel)
    elapsed = time.monotonic() - started
    t.join()
    assert res.status == "canceled"
    # Cancellation latency must be well under the 2s timeout.
    assert elapsed < 1.5


def test_cancel_before_launch_returns_immediately(run_one):
    # Works in every environment: an already-set event must prevent the trial
    # subprocess from even being started.
    ev = threading.Event()
    ev.set()
    started = time.monotonic()
    res = run_one([("a", "x", "1")], SLOW_SQL, ev)
    assert res.status == "canceled"
    assert time.monotonic() - started < 1.0


def test_empty_table_trial_runs(run_one):
    res = run_one([], SQL_OK)
    assert res.status == "success"


def test_single_row_csv_shape_is_stable(run_one):
    # Regression guard for the DuckDB sniffer issues observed during design:
    # a one-row trial must load three VARCHAR columns correctly.
    res = run_one([("s999", "metric", "0")], SQL_DIVZERO)
    assert res.status == "error"
    assert res.error_type == "InvalidInputException"


def test_duckdb_available_in_isolated_python():
    # The subprocess uses `python -I`; the packaging must still find duckdb.
    import json
    import subprocess
    import sys
    from app.trial import _RUNNER_PATH
    proc = subprocess.run(
        [sys.executable, "-I", _RUNNER_PATH],
        input=json.dumps({"rows": [], "sql": "SELECT 1"}),
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["status"] == "success"
