# backend/routes/upload_route.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from pathlib import Path
import shutil
import os
import time

from ..utils.file_utils import ensure_dir, secure_filename  # provided in utils
from ..utils.pipeline_utils import run_pipeline, PIPELINE_ROOT, LOG_DIR  # provided in utils

router = APIRouter(prefix="/api/upload", tags=["upload"])

UPLOAD_DIR = PIPELINE_ROOT / "data" / "uploads"
ensure_dir(UPLOAD_DIR)
ensure_dir(LOG_DIR)


def _save_upload(file: UploadFile) -> Path:
    """
    Save UploadFile safely to UPLOAD_DIR using our secure_filename helper.
    Streams to disk to support large files.
    """
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    fname = secure_filename(file.filename)
    if not fname:
        raise HTTPException(status_code=400, detail="Invalid filename")

    dest = UPLOAD_DIR / fname

    try:
        with dest.open("wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}") from e
    finally:
        try:
            file.file.close()
        except Exception:
            pass

    return dest


@router.post("/", response_class=JSONResponse)
async def upload_file(
    file: UploadFile = File(...),
    run_after_upload: bool = Query(False, description="If true, run the full pipeline after upload"),
    start_at: Optional[str] = Query(None, description="Optional pipeline step label prefix, e.g. 'Step 2'")
) -> Dict[str, Any]:
    """
    Upload a source data file. Optionally run the pipeline end-to-end (or from a step).
    Returns file metadata and (if run) a pointer to the pipeline log.
    """
    saved_path = _save_upload(file)
    meta = {
        "filename": saved_path.name,
        "saved_path": str(saved_path),
        "size_bytes": saved_path.stat().st_size,
        "uploaded_at": int(time.time()),
    }

    result: Dict[str, Any] = {"status": "ok", "message": "File uploaded", "file": meta}

    if run_after_upload:
        # Allow user to start at a specific step (matches run_pipeline START_AT behavior)
        env = os.environ.copy()
        if start_at:
            env["START_AT"] = start_at

        try:
            # Run the master pipeline; logs stream to logs/pipeline.log
            rc = run_pipeline("run_pipeline.py", env_override=env)
            result["pipeline"] = {
                "started": True,
                "return_code": rc,
                "log_path": str(LOG_DIR / "pipeline.log"),
            }
            result["message"] += " & pipeline executed"
        except Exception as e:
            # Pipeline failure is non-fatal to the upload; bubble details
            result["pipeline"] = {
                "started": True,
                "error": str(e),
                "log_path": str(LOG_DIR / "pipeline.log"),
            }
            result["status"] = "error"
            result["message"] += " but pipeline failed"

    return result


@router.get("/list", response_class=JSONResponse)
async def list_uploads() -> Dict[str, Any]:
    """
    List uploaded files with basic metadata for the UI file picker.
    """
    files: List[Dict[str, Any]] = []
    for p in sorted(UPLOAD_DIR.glob("*")):
        if p.is_file():
            files.append({
                "filename": p.name,
                "path": str(p),
                "size_bytes": p.stat().st_size,
                "modified_at": int(p.stat().st_mtime),
            })
    return {"status": "ok", "files": files}


@router.delete("/", response_class=JSONResponse)
async def delete_upload(
    filename: str = Query(..., description="Filename to delete (must exist in uploads dir)")
) -> Dict[str, Any]:
    """
    Delete a previously uploaded file by name.
    """
    target = UPLOAD_DIR / secure_filename(filename)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    try:
        target.unlink()
        return {"status": "ok", "message": f"Deleted {filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete {filename}: {e}")