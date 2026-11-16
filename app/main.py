"""FastAPI application: upload, monitor, cancel, download minimal samples."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    StreamingResponse,
)

from .csvutil import ValidationError, parse_csv
from .jobs import JobManager

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
WORK_ROOT = os.environ.get(
    "SAMPLE_MINIMIZER_WORKDIR", str(PROJECT_ROOT / "data" / "jobs")
)
DEMO_DIR = PROJECT_ROOT / "demo_data"

manager = JobManager(WORK_ROOT)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # Periodically purge expired job artifacts.
    task = asyncio.create_task(_cleanup_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(300)
        manager.cleanup()


app = FastAPI(title="检验数据最小样本缩减工具", version="1.0.0",
              lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((BASE_DIR / "static" / "index.html").read_text(
        encoding="utf-8"))


@app.post("/api/jobs")
async def create_job(
    file: UploadFile = File(...),
    sql: str = Form(...),
):
    content = await file.read()
    try:
        parsed = parse_csv(content)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        job = manager.create_job(parsed, sql)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(job.public())


@app.get("/api/jobs")
async def list_jobs():
    return {"jobs": manager.list_jobs()}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job.public()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    job = manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    before = job.status
    updated = manager.request_cancel(job_id)
    # Immediate acknowledgement; terminal state comes via polling/SSE once
    # every in-flight trial subprocess has actually stopped.
    if updated is not None and updated.status == before and before in {
        "succeeded", "no_error", "timed_out", "failed", "canceled"
    }:
        # A terminal job is immutable. A published success keeps its result
        # and is explicitly not cancelable.
        if before == "succeeded":
            detail = "任务已成功并发布最小样本，结果已生效，不可取消。"
        elif before == "canceled":
            detail = "任务此前已取消。"
        else:
            detail = f"任务已处于终态 {before}，不可取消。"
        raise HTTPException(status_code=409, detail=detail)
    return {"id": job_id, "acknowledged": True, "status": updated.status}


@app.get("/api/jobs/{job_id}/events")
async def job_events(job_id: str):
    job = manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")

    def generator():
        for evt in manager.stream_events(job):
            yield json.dumps(evt, ensure_ascii=False) + "\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache",
                 "X-Accel-Buffering": "no"},
    )


@app.get("/api/jobs/{job_id}/download")
async def download_result(job_id: str):
    job = manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    # Download eligibility is part of the single atomic publish: only a
    # succeeded job with a materialized result file is downloadable. While a
    # result is still being written (or after cancel/timeout/failure) there
    # is nothing to download.
    snapshot = job.public()
    if job.status != "succeeded" or not snapshot["has_result"]:
        raise HTTPException(
            status_code=409,
            detail="仅 succeeded 状态且已发布成品的任务可下载最小样本"
                   "（取消/超时/失败不会发布）",
        )
    if not os.path.exists(job.result_path):
        raise HTTPException(status_code=410, detail="结果文件已过期清理")
    return FileResponse(
        job.result_path,
        media_type="text/csv",
        filename=f"minimal_sample_{job_id[:8]}.csv",
    )


@app.get("/api/demo")
async def demo_info():
    demos = []
    for name in sorted(DEMO_DIR.glob("*.json")):
        demos.append(json.loads(name.read_text(encoding="utf-8")))
    return {"demos": demos}


@app.get("/api/demo/{demo_id}/csv")
async def demo_csv(demo_id: str):
    path = DEMO_DIR / f"{demo_id}.csv"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="演示数据不存在")
    return FileResponse(
        path, media_type="text/csv", filename=f"{demo_id}.csv"
    )
