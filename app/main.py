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
    job = manager.request_cancel(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    # Immediate acknowledgement; terminal state comes via polling/SSE once
    # every in-flight trial subprocess has actually stopped.
    return {"id": job_id, "acknowledged": True, "status": job.status}


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
    if job.status != "succeeded" or not job.result_path:
        raise HTTPException(
            status_code=409,
            detail="仅 succeeded 状态的任务可下载最小样本（取消/超时不会发布）",
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
