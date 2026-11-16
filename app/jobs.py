"""Job orchestration: state machine, background workers and cancel semantics.

Concurrency model
-----------------
Each submitted job runs on its own worker thread.  Trials inside a job run
one at a time as isolated subprocesses (see :mod:`app.trial`), while jobs
themselves are independent: two browser windows submitting two jobs get two
threads, two sets of temp artifacts and two cancel events.  Canceling one job
never touches the other.

State machine
-------------
    queued -> running -> succeeded        (minimal sample published)
                       -> no_error        (original query succeeded)
                       -> timed_out       (a trial exceeded 2 s; no publish)
                       -> failed          (another unrecoverable error)
                       -> canceling -> canceled   (only after trials stop)

Publishing contract (the whole point of this module)
----------------------------------------------------
A minimal sample is the result of a *single atomic publish*:

1. while a job has not reached ``succeeded`` there is no visible result at
   all -- ``has_result`` is false, ``result_rows`` stays 0 and the download
   endpoint refuses the request, even while the result file is being written;
2. the file is written to a temporary name inside the job directory and only
   renamed into place inside the state lock;
3. every terminal transition re-checks the cancel event *under the state
   lock*.  If cancellation was approved, the job ends as ``canceled`` and any
   unpublished/temporary artifact is discarded; the already-published success
   result is kept and a later cancel request is refused;
4. a job only becomes ``canceled`` after every in-flight trial subprocess has
   been stopped.  Canceling never waits on a result write: the write runs on
   the worker thread, the request is acknowledged immediately and every query
   endpoint stays responsive.
"""
from __future__ import annotations

import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .csvutil import ParsedCsv, render_subset_csv, validate_select
from .minimizer import (
    MinimizationCanceled,
    MinimizeOutcome,
    Progress,
    TimeoutDuringMinimization,
    ddmin,
)
from .trial import TRIAL_TIMEOUT_SECONDS, run_trial

# Terminal states.
TERMINAL_STATES = {"succeeded", "no_error", "timed_out", "failed", "canceled"}

# Keep finished jobs around for a while so polls/downloads still work.
JOB_RETENTION_SECONDS = 3600

# Regressions need a *controllable* publish pause: when set (seconds), the
# worker sleeps at the very start of result publishing, i.e. while the sample
# is still unpublished.  It models a slow disk without making real I/O slow.
# Tests additionally use the ``JobManager.publish_gate_factory`` seam, which
# blocks publishing on an Event until the test releases it -- fully
# deterministic, with no dependence on wall-clock scheduling.
PUBLISH_PAUSE_ENV = "SAMPLE_MINIMIZER_PUBLISH_PAUSE_SECONDS"
_TMP_RESULT_SUFFIX = ".tmp"


def _publish_pause_seconds() -> float:
    raw = os.environ.get(PUBLISH_PAUSE_ENV, "")
    if not raw.strip():
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


@dataclass
class Job:
    id: str
    sql: str
    total_rows: int
    created_at: float
    status: str = "queued"
    cancel_event: threading.Event = field(default_factory=threading.Event)
    progress: Progress = field(default_factory=Progress)
    error_type: Optional[str] = None
    first_line: Optional[str] = None
    message: str = ""
    outcome: Optional[MinimizeOutcome] = None
    result_path: Optional[str] = None
    result_rows: int = 0
    baseline_status: Optional[str] = None
    events: "queue.Queue[dict]" = field(default_factory=queue.Queue)
    finished_at: Optional[float] = None

    def __post_init__(self) -> None:
        # Guards every status/result mutation and the read-side snapshots so
        # that cancel requests can never interleave with a terminal publish.
        self.state_lock = threading.RLock()

    def public(self) -> dict:
        # Snapshot the mutable bits under the lock; the (cheap) existence
        # check happens after releasing it so polls never block on a write.
        with self.state_lock:
            status = self.status
            result_path = self.result_path
            result_rows = self.result_rows
            progress = self.progress.snapshot()
            payload = {
                "id": self.id,
                "status": status,
                "total_rows": self.total_rows,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "error_type": self.error_type,
                "first_line": self.first_line,
                "message": self.message,
                "baseline_status": self.baseline_status,
                # Result row count is part of the same atomic publish: it must
                # never be advertised before the succeeded state is visible.
                "result_rows": result_rows if status == "succeeded" else 0,
                "has_result": False,
                "progress": progress,
                "trial_timeout_seconds": TRIAL_TIMEOUT_SECONDS,
            }
        payload["has_result"] = (
            status == "succeeded"
            and result_path is not None
            and os.path.exists(result_path)
        )
        return payload


class JobManager:
    def __init__(self, work_root: str, publish_gate_factory=None) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()
        self._work_root = work_root
        # Optional per-job gate used by regression tests: when the factory
        # returns an Event-like object, publishing blocks until the test sets
        # it (or the job is canceled). Production leaves this as None.
        self.publish_gate_factory = publish_gate_factory
        os.makedirs(work_root, exist_ok=True)

    # ------------------------------------------------------------------ CRUD
    def create_job(self, parsed: ParsedCsv, sql: str) -> Job:
        sql = validate_select(sql)
        job_id = uuid.uuid4().hex
        job_dir = os.path.join(self._work_root, job_id)
        os.makedirs(job_dir, exist_ok=True)

        # Persist the original upload alongside the job for auditability.
        original_path = os.path.join(job_dir, "original.csv")
        with open(original_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(
                render_subset_csv(
                    parsed.rows, parsed.header, parsed.line_terminator
                )
            )
        with open(os.path.join(job_dir, "query.sql"), "w", encoding="utf-8") as fh:
            fh.write(sql)

        job = Job(id=job_id, sql=sql, total_rows=len(parsed.rows),
                  created_at=time.time())
        # Attach the parsed payload out-of-band (not part of public state).
        job._parsed = parsed  # type: ignore[attr-defined]
        job._job_dir = job_dir  # type: ignore[attr-defined]
        with self._lock:
            self._jobs[job_id] = job

        thread = threading.Thread(
            target=self._run, args=(job,), name=f"job-{job_id}", daemon=True
        )
        job._thread = thread  # type: ignore[attr-defined]
        thread.start()
        return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> List[dict]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(), key=lambda j: j.created_at, reverse=True
            )
            return [j.public() for j in jobs]

    # -------------------------------------------------------------- canceling
    def request_cancel(self, job_id: str) -> Optional[Job]:
        """Signal cancellation.

        Acknowledged immediately for every non-terminal job: the cancel event
        is set and the status becomes ``canceling`` while the worker keeps
        running.  The job only reaches the terminal ``canceled`` state after
        all in-flight trials have stopped.  A terminal (in particular an
        already published ``succeeded``) job is immutable and cannot be
        canceled -- its result stays downloadable.
        """
        job = self.get(job_id)
        if job is None:
            return None
        with job.state_lock:
            if job.status in TERMINAL_STATES:
                return job
            job.cancel_event.set()
            if job.status in ("queued", "running", "canceling"):
                job.status = "canceling"
        self._emit(job, {"event": "cancel_requested", "id": job.id})
        return job

    # ---------------------------------------------------------------- events
    def _emit(self, job: Job, evt: dict) -> None:
        job.events.put(evt)

    def stream_events(self, job: Job):
        """Generator yielding queued events plus periodic heartbeats until the
        job reaches a terminal state and the queue has drained."""
        while True:
            try:
                evt = job.events.get(timeout=1.0)
                yield evt
            except queue.Empty:
                yield {"event": "heartbeat", "id": job.id}
            if job.status in TERMINAL_STATES and job.events.empty():
                yield {"event": "terminal", "id": job.id,
                       "status": job.status}
                return

    # -------------------------------------------------------- terminal paths
    def _commit_terminal(
        self,
        job: Job,
        status: str,
        message: str,
        *,
        discard_result: bool,
        extra_emit: Optional[dict] = None,
    ) -> str:
        """Single choke point for every terminal transition.

        Returns the status that was actually committed.  A pending cancel
        always wins over a non-cancel terminal transition, and a job that
        already published its result stays ``succeeded`` forever.
        """
        assert status in TERMINAL_STATES
        with job.state_lock:
            current = job.status
            if current in TERMINAL_STATES:
                return current
            if status != "canceled" and job.cancel_event.is_set():
                status = "canceled"
            job.status = status
            if status == "canceled":
                message = "任务已取消，所有试跑均已停止；不会发布最小样本。"
            job.message = message
            job.finished_at = time.time()
            if status == "canceled" or discard_result:
                # Unpublished samples are never handed out after cancel (and
                # timed-out/failed jobs never publish either).
                job.result_path = None
                job.result_rows = 0
                self._discard_unpublished(job)
            result_rows = job.result_rows
        if extra_emit is not None:
            self._emit(job, extra_emit)
        self._emit(job, {
            "event": "terminal",
            "id": job.id,
            "status": status,
            "result_rows": result_rows,
        })
        return status

    def _discard_unpublished(self, job: Job) -> None:
        """Remove temporary/result files that were never committed.

        ``original.csv`` and ``query.sql`` are audit artifacts of the upload
        and are kept; anything else (the in-progress ``*.tmp`` write or a
        stale result file) is dropped so a canceled job cannot leave a
        downloadable sample behind.
        """
        job_dir = getattr(job, "_job_dir", "")
        if not job_dir or not os.path.isdir(job_dir):
            return
        for name in os.listdir(job_dir):
            if name in ("original.csv", "query.sql"):
                continue
            try:
                os.remove(os.path.join(job_dir, name))
            except OSError:
                pass

    def _finish_canceled(self, job: Job) -> str:
        """Publish the canceled state only after trials are stopped.

        The trial function blocks until the subprocess group is reaped, so by
        the time MinimizationCanceled propagates here nothing is running.
        """
        job.cancel_event.set()
        return self._commit_terminal(
            job,
            "canceled",
            "任务已取消，所有试跑均已停止；不会发布最小样本。",
            discard_result=True,
        )

    def _finish_timeout(self, job: Job, stage: str) -> str:
        return self._commit_terminal(
            job,
            "timed_out",
            f"有试跑超过 {TRIAL_TIMEOUT_SECONDS:.0f} 秒上限（阶段：{stage}），"
            "任务未完成，不发布最小样本。",
            discard_result=True,
        )

    def _set_running(self, job: Job) -> None:
        with job.state_lock:
            if job.status in TERMINAL_STATES or job.cancel_event.is_set():
                return
            job.status = "running"
        self._emit(job, {"event": "status", "id": job.id,
                         "status": "running"})

    def _publish_result(
        self, job: Job, parsed: ParsedCsv, outcome: MinimizeOutcome, job_dir: str
    ) -> str:
        """Atomically publish the minimal sample, honoring a late cancel.

        The slow part (rendering/writing the file) happens *outside* the state
        lock on the worker thread, so cancellation, polling and other jobs
        stay responsive while a slow disk is being written.  The final
        decision -- publish as ``succeeded`` or discard as ``canceled`` -- is
        taken under the lock and is therefore race-free.
        """
        subset_rows = [parsed.rows[i] for i in outcome.indices]
        rendered = render_subset_csv(
            subset_rows, parsed.header, parsed.line_terminator
        )
        final_path = os.path.join(job_dir, "minimal_sample.csv")
        tmp_path = final_path + _TMP_RESULT_SUFFIX

        # Controllable publish pause for regression tests: while blocked here
        # the sample is still unpublished and cancel must remain immediately
        # serviceable from the API threads. Two mechanisms:
        #   * an optional per-job gate Event the test releases explicitly,
        #     which is fully deterministic (no wall-clock assumptions);
        #   * an env-var sleep modeling a slow disk.
        # Both waits are cancel-aware so an approved cancel wakes the worker
        # promptly to discard the unpublished sample.
        gate = None
        if self.publish_gate_factory is not None:
            gate = self.publish_gate_factory(job)
        if gate is not None:
            # Park on the gate in short slices so an approved cancel wakes the
            # worker promptly; releasing the gate resumes publishing.
            while not gate.is_set() and not job.cancel_event.is_set():
                gate.wait(timeout=0.05)
        pause = _publish_pause_seconds()
        if pause:
            self._interruptible_wait(job, pause)
        with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(rendered)
            fh.flush()
            os.fsync(fh.fileno())

        with job.state_lock:
            if job.status in TERMINAL_STATES:
                # Another terminal path won the race; drop the unpublished
                # file and keep the existing state.
                self._safe_remove(tmp_path)
                return job.status
            if job.cancel_event.is_set():
                # Cancellation was approved while the write was in flight:
                # discard the unpublished sample, never make it visible.
                self._safe_remove(tmp_path)
                self._safe_remove(final_path)
                return self._finish_canceled(job)
            # The single atomic publish: rename into place and expose the
            # result together with the succeeded state under one lock.
            os.replace(tmp_path, final_path)
            job.result_path = final_path
            job.result_rows = len(subset_rows)
            job.status = "succeeded"
            job.message = (
                f"已得到 1-最小样本：{job.total_rows} 行缩减至 "
                f"{job.result_rows} 行，共 {outcome.trials_completed} 次试跑。"
            )
            job.finished_at = time.time()
            result_rows = job.result_rows
        self._emit(job, {"event": "terminal", "id": job.id,
                         "status": "succeeded",
                         "result_rows": result_rows})
        return "succeeded"

    @staticmethod
    def _safe_remove(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass

    @staticmethod
    def _interruptible_wait(job: Job, seconds: float) -> None:
        """Sleep up to ``seconds`` but return as soon as cancel is requested."""
        if job.cancel_event.wait(timeout=seconds):
            return

    # ---------------------------------------------------------------- worker
    def _run(self, job: Job) -> None:
        parsed: ParsedCsv = job._parsed  # type: ignore[attr-defined]
        job_dir: str = job._job_dir  # type: ignore[attr-defined]

        def run_one_trial(rows):
            return run_trial(rows, job.sql, job.cancel_event)

        try:
            if job.cancel_event.is_set():
                self._finish_canceled(job)
                return

            self._set_running(job)
            if job.status in TERMINAL_STATES:
                return

            # ------------------------------------------------------ baseline
            job.progress.phase = "baseline"
            baseline = run_one_trial(parsed.rows)

            if baseline.status == "canceled":
                self._finish_canceled(job)
                return
            if baseline.status == "timeout":
                self._finish_timeout(job, stage="baseline")
                return
            if baseline.status == "success":
                # Original query works: nothing to reduce.
                job.baseline_status = "success"
                self._commit_terminal(
                    job,
                    "no_error",
                    "原始查询执行成功，未出现异常，无需缩减。",
                    discard_result=True,
                )
                return
            if baseline.status != "error":
                self._commit_terminal(
                    job,
                    "failed",
                    f"基线试跑异常（{baseline.status}），无法开始缩减。",
                    discard_result=True,
                )
                return

            # Baseline error: this is the fingerprint every later trial must
            # reproduce exactly.
            job.baseline_status = "error"
            job.error_type = baseline.error_type
            job.first_line = baseline.first_line
            job.progress.last_error_type = baseline.error_type
            job.progress.last_first_line = baseline.first_line
            self._emit(job, {
                "event": "baseline_error",
                "id": job.id,
                "error_type": baseline.error_type,
                "first_line": baseline.first_line,
            })

            # ----------------------------------------------- minimization
            outcome = ddmin(
                rows=parsed.rows,
                error_type=job.error_type,
                first_line=job.first_line or "",
                run=run_one_trial,
                progress=job.progress,
                canceled_event=job.cancel_event,
            )
            job.outcome = outcome

            # --------------------------------------------- publish artifact
            self._publish_result(job, parsed, outcome, job_dir)

        except MinimizationCanceled:
            self._finish_canceled(job)
        except TimeoutDuringMinimization:
            self._finish_timeout(job, stage="minimize")
        except Exception as exc:  # noqa: BLE001 - surface as failed job
            self._commit_terminal(
                job,
                "failed",
                f"任务失败：{type(exc).__name__}: {exc}",
                discard_result=True,
            )

    # ------------------------------------------------------------- housekeep
    def cleanup(self) -> None:
        now = time.time()
        with self._lock:
            expired = [
                j for j in self._jobs.values()
                if j.status in TERMINAL_STATES
                and j.finished_at
                and now - j.finished_at > JOB_RETENTION_SECONDS
            ]
            for job in expired:
                self._jobs.pop(job.id, None)
                job_dir = getattr(job, "_job_dir", "")
                if job_dir and os.path.isdir(job_dir):
                    for name in os.listdir(job_dir):
                        try:
                            os.remove(os.path.join(job_dir, name))
                        except OSError:
                            pass
                    try:
                        os.rmdir(job_dir)
                    except OSError:
                        pass
