"""Delta-debugging minimizer (Zeller/Hildebrandt ddmin).

Given an ordered list of rows and a deterministic SELECT whose execution on
the full list raises a known exception, ``ddmin`` repeatedly removes rows
until the remaining subset is **1-minimal**: the same exception class and
first-line message still occurs, but deleting any single remaining row makes
the error disappear.

Row order and duplicate rows are always preserved: subsets are represented as
sorted index lists over the original rows, and the result is materialized by
index lookup, never by set operations on row content.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .trial import TrialResult

# Signature of the trial callback: rows to test -> TrialResult.
TrialFn = Callable[[List[List[str]]], TrialResult]


@dataclass
class Progress:
    """Mutable progress snapshot shared with the web layer."""

    total_input_rows: int = 0
    trials_completed: int = 0
    current_rows: int = 0
    granularity: int = 2
    phase: str = "init"          # init | reduce | verify_single | done
    last_result: str = ""        # short status of the last trial
    last_error_type: Optional[str] = None
    last_first_line: Optional[str] = None
    history: List[dict] = field(default_factory=list)

    def snapshot(self) -> dict:
        return {
            "total_input_rows": self.total_input_rows,
            "trials_completed": self.trials_completed,
            "current_rows": self.current_rows,
            "granularity": self.granularity,
            "phase": self.phase,
            "last_result": self.last_result,
            "last_error_type": self.last_error_type,
            "last_first_line": self.last_first_line,
            "history": list(self.history[-50:]),
        }


class TimeoutDuringMinimization(Exception):
    """Raised when a trial exceeds the per-trial 2 second limit."""


class MinimizationCanceled(Exception):
    """Raised when the job's cancel event is observed."""


@dataclass
class MinimizeOutcome:
    indices: List[int]
    trials_completed: int
    one_minimal: bool


def _matches(result: TrialResult, error_type: str, first_line: str) -> bool:
    return (
        result.reproduces
        and result.error_type == error_type
        and (result.first_line or "") == first_line
    )


def ddmin(
    rows: List[List[str]],
    error_type: str,
    first_line: str,
    run: TrialFn,
    progress: Progress,
    canceled_event,
) -> MinimizeOutcome:
    """Run ddmin and return the 1-minimal subset as original-row indices."""
    n = len(rows)
    progress.total_input_rows = n
    progress.current_rows = n

    def check_cancel() -> None:
        if canceled_event is not None and canceled_event.is_set():
            raise MinimizationCanceled()

    def evaluate(indices: List[int], phase_label: str) -> TrialResult:
        """Execute one trial and translate raw statuses to spec semantics."""
        check_cancel()
        progress.phase = phase_label
        subset = [rows[i] for i in indices]
        result = run(subset)

        progress.trials_completed += 1
        progress.current_rows = len(indices)
        progress.last_result = result.status
        progress.history.append(
            {
                "trial": progress.trials_completed,
                "rows_tested": len(indices),
                "status": result.status,
                "error_type": result.error_type,
            }
        )

        if result.status == "canceled":
            raise MinimizationCanceled()
        if result.status == "timeout":
            # The task is incomplete: no minimal sample may be published.
            raise TimeoutDuringMinimization()
        if result.status in ("runner_crash",):
            # Infrastructure failure is not a reproduction; treat like a
            # non-reproducing outcome but keep it visible in the history.
            return result
        return result

    # Empty-table sanity check (cheap; also covers degenerate inputs).
    empty_result = evaluate([], "reduce")
    if _matches(empty_result, error_type, first_line):
        # The error does not depend on any data row at all.
        progress.phase = "done"
        return MinimizeOutcome([], progress.trials_completed, True)

    # Classical ddmin over the index list [0 .. n-1].
    current: List[int] = list(range(n))
    granularity = 2
    progress.granularity = granularity

    while len(current) >= 2:
        check_cancel()
        block_size = max(1, len(current) // granularity)
        blocks = [
            current[i : i + block_size]
            for i in range(0, len(current), block_size)
        ]
        # Last block can be smaller; that is expected.

        reduced = False

        # Phase 1: try removing each block (complement must still fail).
        for block in blocks:
            block_set = set(block)
            complement = [i for i in current if i not in block_set]
            result = evaluate(complement, "reduce")
            if _matches(result, error_type, first_line):
                current = complement
                granularity = max(granularity - 1, 2)
                reduced = True
                break

        if reduced:
            progress.granularity = granularity
            continue

        # Phase 2: try each block alone (the subset must still fail).
        for block in blocks:
            result = evaluate(list(block), "reduce")
            if _matches(result, error_type, first_line):
                current = list(block)
                granularity = 2
                reduced = True
                break

        if reduced:
            progress.granularity = granularity
            continue

        # Neither complements nor individual blocks reproduce.
        if granularity < len(current):
            granularity = min(granularity * 2, len(current))
            progress.granularity = granularity
            continue

        break  # granularity == len(current): every single row was tried alone

    # Explicit 1-minimality verification: deleting ANY single remaining row
    # must fail to reproduce the original error.
    progress.phase = "verify_single"
    one_minimal = True
    if len(current) >= 2:
        for pos in range(len(current)):
            check_cancel()
            reduced_indices = current[:pos] + current[pos + 1 :]
            result = evaluate(reduced_indices, "verify_single")
            if _matches(result, error_type, first_line):
                # Should be unreachable after ddmin, but the spec demands the
                # guarantee, so keep reducing instead of publishing a bad set.
                current = reduced_indices
                one_minimal = False
        # If we found a further reduction, repeat the whole process.
        if not one_minimal:
            return ddmin(
                rows, error_type, first_line, run, progress, canceled_event
            )

    progress.phase = "done"
    return MinimizeOutcome(
        indices=current,
        trials_completed=progress.trials_completed,
        one_minimal=True,
    )
