"""ddmin minimizer: correctness, 1-minimality and ordering guarantees."""
import threading

import pytest

from app.minimizer import (
    MinimizationCanceled,
    Progress,
    TimeoutDuringMinimization,
    ddmin,
)
from app.trial import TrialResult, run_trial

SQL_DIVZERO = (
    "SELECT item, SUM(1 // CAST(value AS INTEGER)) "
    "FROM readings GROUP BY item"
)
SQL_CAST = (
    "SELECT item, CAST(MAX(value) AS INTEGER) "
    "FROM readings GROUP BY item HAVING count(*) >= 2"
)


def _real_run(rows, sql, ev):
    def _fn(subset):
        return run_trial(subset, sql, ev)
    return _fn


def test_minimizes_divzero_to_one_row():
    rows = [["s%d" % i, "m", "5"] for i in range(49)]
    rows.append(["s49", "m", "0"])
    ev = threading.Event()
    baseline = run_trial(rows, SQL_DIVZERO, ev)
    assert baseline.status == "error"
    progress = Progress()
    outcome = ddmin(
        rows, baseline.error_type, baseline.first_line or "",
        _real_run(rows, SQL_DIVZERO, ev), progress, ev,
    )
    assert len(outcome.indices) == 1
    assert outcome.indices == [49]
    assert outcome.one_minimal is True


def test_result_is_one_minimal_by_exhaustive_check():
    # Small enough to verify every single-row deletion independently.
    rows = [["r%d" % i, "g", ("0" if i in (2, 6) else str(i + 1))]
            for i in range(10)]
    ev = threading.Event()
    baseline = run_trial(rows, SQL_DIVZERO, ev)
    assert baseline.status == "error"
    progress = Progress()
    outcome = ddmin(
        rows, baseline.error_type, baseline.first_line or "",
        _real_run(rows, SQL_DIVZERO, ev), progress, ev,
    )
    subset = [rows[i] for i in outcome.indices]

    # 1) The subset itself reproduces the exact error.
    res = run_trial(subset, SQL_DIVZERO, ev)
    assert res.status == "error"
    assert res.error_type == "InvalidInputException"

    # 2) Removing ANY one remaining row makes it disappear.
    for pos in range(len(subset)):
        smaller = subset[:pos] + subset[pos + 1:]
        r = run_trial(smaller, SQL_DIVZERO, ev)
        assert r.status == "success", (
            f"deleting row {pos} still reproduced the error: {r}"
        )


def test_cast_error_needs_all_bad_rows():
    # Genuine 2-row 1-minimal dependency: the HAVING gate (count >= 2) only
    # passes while BOTH rows of the "p" group survive; with a single row the
    # group is filtered out, so no CAST error occurs. Deleting either row
    # makes the error disappear -> both rows must be retained.
    rows = []
    for i in range(20):
        rows.append(["d%02d" % i, "f", str(10 + i % 5)])
    target_good = ["g1", "p", "7"]
    target_bad = ["b1", "p", "N/A"]
    rows.extend([target_good, target_bad])
    ev = threading.Event()
    baseline = run_trial(rows, SQL_CAST, ev)
    assert baseline.status == "error"
    outcome = ddmin(
        rows, baseline.error_type, baseline.first_line or "",
        _real_run(rows, SQL_CAST, ev), Progress(), ev,
    )
    assert len(outcome.indices) == 2
    kept = [rows[i] for i in outcome.indices]
    assert kept == [target_good, target_bad]  # original row order preserved


def test_row_order_and_duplicates_preserved():
    # Duplicate zero rows: minimal subset may need only one, but whenever
    # several remain their relative order must match the input.
    rows = [["s%d" % i, "m", ("0" if i in (3, 7) else "4")]
            for i in range(10)]
    ev = threading.Event()
    baseline = run_trial(rows, SQL_DIVZERO, ev)
    outcome = ddmin(
        rows, baseline.error_type, baseline.first_line or "",
        _real_run(rows, SQL_DIVZERO, ev), Progress(), ev,
    )
    assert outcome.indices == sorted(outcome.indices)
    materialized = [rows[i] for i in outcome.indices]
    assert all(r in rows for r in materialized)


def test_duplicates_kept_when_both_required():
    # A contrived fingerprint where two identical rows are both needed:
    # use a mock evaluator instead of SQL semantics.
    rows = [["a", "x", "1"], ["a", "x", "1"], ["b", "x", "2"]]
    calls = []

    def fake_run(subset):
        calls.append(len(subset))
        # Reproduces only when both duplicate rows are present.
        if sum(1 for r in subset if r == ["a", "x", "1"]) >= 2:
            return TrialResult(status="error", error_type="E",
                               first_line="boom")
        return TrialResult(status="success")

    outcome = ddmin(rows, "E", "boom", fake_run, Progress(),
                    threading.Event())
    kept = [rows[i] for i in outcome.indices]
    assert kept == [["a", "x", "1"], ["a", "x", "1"]]


def test_different_exception_does_not_count_as_reproduction():
    rows = [["a", "x", "0"]]

    def fake_run(subset):
        # Always raises, but with a different exception class.
        return TrialResult(status="error", error_type="OtherException",
                           first_line="Invalid Input Error: Division by zero")

    outcome = ddmin(rows, "InvalidInputException",
                    "Invalid Input Error: Division by zero",
                    fake_run, Progress(), threading.Event())
    # Nothing reproduces -> ddmin cannot reduce; original indices retained.
    assert outcome.indices == [0]


def test_different_first_line_does_not_count():
    rows = [["a", "x", "0"], ["b", "x", "1"]]

    def fake_run(subset):
        if len(subset) == 2:
            return TrialResult(status="error", error_type="InvalidInputException",
                               first_line="target message")
        return TrialResult(status="error", error_type="InvalidInputException",
                           first_line="some other message")

    outcome = ddmin(rows, "InvalidInputException", "target message",
                    fake_run, Progress(), threading.Event())
    assert outcome.indices == [0, 1]  # cannot reduce


def test_timeout_aborts_and_does_not_publish():
    rows = [["a", "x", "0"], ["b", "x", "1"]]

    def fake_run(subset):
        return TrialResult(status="timeout")

    with pytest.raises(TimeoutDuringMinimization):
        ddmin(rows, "E", "boom", fake_run, Progress(), threading.Event())


def test_cancel_aborts_minimization():
    ev = threading.Event()
    ev.set()
    with pytest.raises(MinimizationCanceled):
        ddmin([["a", "x", "1"]], "E", "boom",
              lambda s: TrialResult(status="success"),
              Progress(), ev)


def test_progress_tracks_trials():
    rows = [["s%d" % i, "m", "5"] for i in range(8)]
    rows.append(["s8", "m", "0"])
    progress = Progress()
    ev = threading.Event()
    baseline = run_trial(rows, SQL_DIVZERO, ev)
    ddmin(rows, baseline.error_type, baseline.first_line or "",
          _real_run(rows, SQL_DIVZERO, ev), progress, ev)
    assert progress.trials_completed > 0
    assert progress.phase == "done"
    assert len(progress.history) == progress.trials_completed
