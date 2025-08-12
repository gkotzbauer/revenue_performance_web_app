# backend/routes/logs_route.py
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from typing import List, Dict, Any
from pathlib import Path
import os
import time

from ..utils.file_utils import ensure_dirs

router = APIRouter(prefix="/api/logs", tags=["logs"])

# Define constants locally since they're not in pipeline_utils
PIPELINE_ROOT = Path(__file__).resolve().parents[2]  # repo root
LOG_DIR = PIPELINE_ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

ensure_dirs()
LOG_DIR.mkdir(parents=True, exist_ok=True)


@router.get("/latest", response_class=JSONResponse)
async def get_latest_logs(
    tail_lines: int = Query(50, description="Number of log lines to return from the end of the file.")
) -> Dict[str, Any]:
    """
    Return the last `tail_lines` lines from the pipeline log file.
    """
    if not LOG_FILE.exists():
        return {"status": "ok", "logs": [], "message": "No logs yet."}

    try:
        with LOG_FILE.open("r") as f:
            lines = f.readlines()
        tail = lines[-tail_lines:] if tail_lines > 0 else lines
        return {"status": "ok", "logs": tail}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {e}")


@router.get("/stream")
async def stream_logs(poll_interval: float = 1.0):
    """
    Stream pipeline logs in near-real-time.
    Client keeps the connection open and receives logs as they are written.
    """
    if not LOG_FILE.exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    def log_generator():
        with LOG_FILE.open("r") as f:
            # Seek to the end of the file initially
            f.seek(0, 2)
            while True:
                line = f.readline()
                if line:
                    yield line
                else:
                    time.sleep(poll_interval)

    return StreamingResponse(log_generator(), media_type="text/plain")


@router.delete("/clear", response_class=JSONResponse)
async def clear_logs() -> Dict[str, Any]:
    """
    Clear the pipeline log file.
    """
    try:
        if LOG_FILE.exists():
            LOG_FILE.unlink()
        LOG_FILE.touch()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear logs: {e}")

    return {"status": "ok", "message": "Logs cleared"}