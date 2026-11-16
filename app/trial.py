"""Subprocess management for individual trials.

Each trial is an independent process (and process group) with its own
in-memory DuckDB instance.  The parent enforces the hard 2 second limit and
cancels promptly.

Two-tier termination
--------------------
1. **Signal-based (primary).**  The trial runs in a fresh process group; on
   cancellation/timeout the parent sends SIGKILL to the whole group.  This is
   immediate and is how the application behaves inside the Docker container.
2. **Pre-launch cancel check (restricted sandboxes).**  Some hardened
   environments deny the parent permission to signal a live child.  The
   parent therefore re-checks the cancel event before launching every trial,
   so no new trial ever starts after cancellation, and the job reaches the
   ``canceled`` state once the already-running trial returns on its own.
"""
from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional

# Hard per-trial wall-clock budget required by the specification.
TRIAL_TIMEOUT_SECONDS = 2.0

_RUNNER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runner.py")


@dataclass
class TrialResult:
    """Outcome of one trial.

    status is one of:
      ``success``       - the query ran without error
      ``error``         - the query raised (see error_type / first_line)
      ``timeout``       - the trial exceeded the 2 second limit
      ``canceled``      - the job was canceled while the trial was running
      ``runner_crash``  - the runner process itself died unexpectedly
    """

    status: str
    elapsed_ms: float = 0.0
    error_type: Optional[str] = None
    first_line: Optional[str] = None

    @property
    def reproduces(self) -> bool:
        """Only an identical exception class + first-line message counts."""
        return self.status == "error" and self.error_type is not None


def _kill_group(proc: subprocess.Popen) -> bool:
    """Kill the trial process group. Returns True if a signal was delivered."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        return True
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
            return True
        except (ProcessLookupError, PermissionError):
            return False


def run_trial(
    rows: list[list[str]],
    sql: str,
    canceled_event,
    timeout_seconds: float = TRIAL_TIMEOUT_SECONDS,
) -> TrialResult:
    """Run one trial in an isolated subprocess.

    ``canceled_event`` is a :class:`threading.Event`-like object. If it is
    already set, no subprocess is launched; if it becomes set while a trial is
    running the process group is killed immediately where the platform allows
    it, otherwise the job simply stops launching any further trials.
    """
    # Do not even launch a new trial once cancellation was requested: this is
    # the boundary that makes cancellation immediate between trials even when
    # the sandbox forbids signaling an already-running process.
    if canceled_event is not None and canceled_event.is_set():
        return TrialResult(status="canceled")

    payload = json.dumps({"rows": rows, "sql": sql})
    try:
        proc = subprocess.Popen(
            [sys.executable, "-I", _RUNNER_PATH],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,  # new process group -> clean group kill
            text=True,
        )
    except OSError as exc:
        return TrialResult(status="runner_crash", first_line=str(exc))

    deadline_poll = 0.02  # poll interval keeps cancellation latency low
    timed_out = False
    signaled = False
    try:
        assert proc.stdin is not None
        try:
            proc.stdin.write(payload)
            proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass

        # Poll instead of communicate(): this keeps cancellation latency low
        # (an in-flight trial must be stopped as soon as the user cancels).
        deadline = time.monotonic() + timeout_seconds
        chunks: list[str] = []
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            if canceled_event is not None and canceled_event.is_set():
                signaled = _kill_group(proc)
                try:
                    proc.wait(timeout=2.0 if signaled else timeout_seconds)
                except subprocess.TimeoutExpired:
                    pass
                if signaled:
                    return TrialResult(status="canceled")
                # Signals are denied here: wait for the in-flight query to
                # return on its own, then report cancellation. No further
                # trial will be launched thanks to the pre-launch check.
                break
            if time.monotonic() >= deadline:
                timed_out = True
                signaled = _kill_group(proc)
                # Where signals work the group is already dead and wait()
                # returns immediately. Where they are denied we wait for the
                # in-flight query to finish so the process is never orphaned;
                # the result is still classified strictly as a timeout and no
                # minimal sample is published.
                try:
                    proc.wait(timeout=30.0)
                except subprocess.TimeoutExpired:
                    pass
                break
            # Drain stdout opportunistically so the pipe cannot fill and
            # block the child; keep the data for parsing after exit.
            if proc.stdout is not None:
                ready, _, _ = select.select([proc.stdout], [], [], deadline_poll)
                if ready:
                    try:
                        data = proc.stdout.readline()
                        if data:
                            chunks.append(data)
                    except (ValueError, OSError):
                        pass
            else:
                time.sleep(deadline_poll)

        if proc.stdout is not None:
            try:
                tail = proc.stdout.read()
                if tail:
                    chunks.append(tail)
            except (ValueError, OSError):
                pass
        stdout = "".join(chunks)
    finally:
        if proc.poll() is None:
            if _kill_group(proc):
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    pass

    if canceled_event is not None and canceled_event.is_set():
        return TrialResult(status="canceled")
    if timed_out:
        return TrialResult(status="timeout", elapsed_ms=timeout_seconds * 1000)
    if proc.returncode != 0:
        return TrialResult(
            status="runner_crash",
            error_type="RunnerCrash",
            first_line=(stdout.strip().splitlines() or ["runner exited with error"])[-1][:500],
        )

    line = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return TrialResult(status="runner_crash", first_line="unparseable runner output")

    status = data.get("status", "runner_crash")
    if status == "success":
        return TrialResult(status="success", elapsed_ms=float(data.get("elapsed_ms", 0.0)))
    if status == "error":
        return TrialResult(
            status="error",
            error_type=data.get("error_type", "Unknown"),
            first_line=data.get("first_line", ""),
            elapsed_ms=float(data.get("elapsed_ms", 0.0)),
        )
    return TrialResult(
        status="runner_crash",
        error_type=data.get("error_type", "RunnerCrash"),
        first_line=data.get("first_line", "runner crash"),
    )
