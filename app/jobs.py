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

A job only becomes ``canceled`` *after* every in-flight trial subprocess has
been stopped.  Once canceled, no result file is ever published.
"""
from __future__ import annotations

import json
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

    def public(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "total_rows": self.total_rows,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "error_type": self.error_type,
            "first_line": self.first_line,
            "message": self.message,
            "baseline_status": self.baseline_status,
            "result_rows": self.result_rows,
            "has_result": self.result_path is not None
            and os.path.exists(self.result_path),
            "progress": self.progress.snapshot(),
            "trial_timeout_seconds": TRIAL_TIMEOUT_SECONDS,
        }


class JobManager:
    def __init__(self, work_root: str) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()
        self._work_root = work_root
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
        """Signal cancellation. Acknowledged immediately; the job only reaches
        the ``canceled`` state after all in-flight trials have stopped."""
        job = self.get(job_id)
        if job is None:
            return None
        if job.status not in TERMINAL_STATES:
            job.cancel_event.set()
            if job.status == "queued":
                # Not started yet: the worker observes the event on startup.
                job.status = "canceling"
            elif job.status == "running":
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

    # ---------------------------------------------------------------- worker
    def _run(self, job: Job) -> None:
        parsed: ParsedCsv = job._parsed  # type: ignore[attr-defined]
        job_dir: str = job._job_dir  # type: ignore[attr-defined]

        def _run(rows):
            return run_trial(rows, job.sql, job.cancel_event)
        try:
            if job.cancel_event.is_set():
                self._finish_canceled(job)
                return

            job.status = "running"
            self._emit(job, {"event": "status", "id": job.id,
                             "status": "running"})

            # ------------------------------------------------------ baseline
            job.progress.phase = "baseline"
            baseline = _run(parsed.rows)

            if baseline.status == "canceled":
                self._finish_canceled(job)
                return
            if baseline.status == "timeout":
                self._finish_timeout(job, stage="baseline")
                return
            if baseline.status == "success":
                # Original query works: nothing to reduce.
                job.baseline_status = "success"
                job.status = "no_error"
                job.message = "原始查询执行成功，未出现异常，无需缩减。"
                job.finished_at = time.time()
                self._emit(job, {"event": "terminal", "id": job.id,
                                 "status": "no_error"})
                return
            if baseline.status != "error":
                job.status = "failed"
                job.message = f"基线试跑异常（{baseline.status}），无法开始缩减。"
                job.finished_at = time.time()
                self._emit(job, {"event": "terminal", "id": job.id,
                                 "status": "failed"})
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
            def trial_fn(subset_rows):
                return _run(subset_rows)

            outcome = ddmin(
                rows=parsed.rows,
                error_type=job.error_type,
                first_line=job.first_line or "",
                run=trial_fn,
                progress=job.progress,
                canceled_event=job.cancel_event,
            )
            job.outcome = outcome

            # --------------------------------------------- publish artifact
            subset_rows = [parsed.rows[i] for i in outcome.indices]
            result_path = os.path.join(job_dir, "minimal_sample.csv")
            with open(result_path, "w", encoding="utf-8", newline="") as fh:
                fh.write(render_subset_csv(
                    subset_rows, parsed.header, parsed.line_terminator
                ))
            job.result_path = result_path
            job.result_rows = len(subset_rows)

            job.status = "succeeded"
            job.message = (
                f"已得到 1-最小样本：{job.total_rows} 行缩减至 "
                f"{job.result_rows} 行，共 {outcome.trials_completed} 次试跑。"
            )
            job.finished_at = time.time()
            self._emit(job, {"event": "terminal", "id": job.id,
                             "status": "succeeded",
                             "result_rows": job.result_rows})

        except MinimizationCanceled:
            self._finish_canceled(job)
        except TimeoutDuringMinimization:
            self._finish_timeout(job, stage="minimize")
        except Exception as exc:  # noqa: BLE001 - surface as failed job
            job.status = "failed"
            job.message = f"任务失败：{type(exc).__name__}: {exc}"
            job.finished_at = time.time()
            self._emit(job, {"event": "terminal", "id": job.id,
                             "status": "failed", "message": job.message})

    def _finish_canceled(self, job: Job) -> None:
        """Publish the canceled state only after trials are stopped.

        The trial function blocks until the subprocess group is reaped, so by
        the time MinimizationCanceled propagates here nothing is running.
        """
        # Belt-and-braces: ensure the event is set and give the worker a beat.
        job.cancel_event.set()
        job.status = "canceled"
        job.message = "任务已取消，所有试跑均已停止；不会发布最小样本。"
        job.result_path = None  # never publish after cancel
        job.finished_at = time.time()
        self._emit(job, {"event": "terminal", "id": job.id,
                         "status": "canceled"})

    def _finish_timeout(self, job: Job, stage: str) -> None:
        job.status = "timed_out"
        job.message = (
            f"有试跑超过 {TRIAL_TIMEOUT_SECONDS:.0f} 秒上限（阶段：{stage}），"
            "任务未完成，不发布最小样本。"
        )
        job.result_path = None
        job.finished_at = time.time()
        self._emit(job, {"event": "terminal", "id": job.id,
                         "status": "timed_out"})

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
