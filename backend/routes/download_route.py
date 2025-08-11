# backend/routes/download_route.py
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from typing import Dict, Any, List, Optional
from pathlib import Path
import mimetypes
import os

from ..utils.file_utils import ensure_dir, secure_filename
from ..utils.pipeline_utils import PIPELINE_ROOT

router = APIRouter(prefix="/api/download", tags=["download"])

OUTPUT_DIR = PIPELINE_ROOT / "data" / "outputs"
ensure_dir(OUTPUT_DIR)


def _resolve_output_path(name_or_relpath: str) -> Path:
    """
    Resolve a filename or relative path safely within OUTPUT_DIR.
    - Allows subfolders (relative only).
    - Blocks path traversal outside OUTPUT_DIR.
    """
    # If the client passes a path with separators, normalize it;
    # else treat as a plain filename and sanitize.
    rel = Path(name_or_relpath)
    if rel.parent == Path("."):
        # Plain filename → sanitize
        safe_name = secure_filename(rel.name)
        candidate = OUTPUT_DIR / safe_name
    else:
        # Relative path → normalize & prevent traversal
        candidate = (OUTPUT_DIR / rel).resolve()

    # Security: must live under OUTPUT_DIR
    if not str(candidate).startswith(str(OUTPUT_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {name_or_relpath}")

    return candidate


@router.get("/list", response_class=JSONResponse)
async def list_outputs(
    recursive: bool = Query(False, description="List files in subfolders as well.")
) -> Dict[str, Any]:
    """
    List available output files for download.
    """
    files: List[Dict[str, Any]] = []

    if recursive:
        iterator = OUTPUT_DIR.rglob("*")
    else:
        iterator = OUTPUT_DIR.glob("*")

    for p in sorted(iterator):
        if p.is_file():
            rel_path = p.relative_to(OUTPUT_DIR)
            files.append({
                "name": p.name,
                "relpath": str(rel_path),
                "size_bytes": p.stat().st_size,
                "modified_at": int(p.stat().st_mtime),
                "mime": mimetypes.guess_type(p.name)[0] or "application/octet-stream",
            })

    return {"status": "ok", "root": str(OUTPUT_DIR), "files": files}


@router.get("/file", response_class=FileResponse)
async def download_file(
    filename: str = Query(..., description="Filename or relative path under /data/outputs")
):
    """
    Download a single file by name or relative path under OUTPUT_DIR.
    """
    target = _resolve_output_path(filename)
    media_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
    return FileResponse(
        path=str(target),
        media_type=media_type,
        filename=target.name
    )


@router.delete("/file", response_class=JSONResponse)
async def delete_output(
    filename: str = Query(..., description="Filename or relative path under /data/outputs")
) -> Dict[str, Any]:
    """
    Delete an output file (useful to keep the workspace clean).
    """
    target = _resolve_output_path(filename)
    try:
        os.remove(target)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete: {e}")

    return {"status": "ok", "message": f"Deleted {filename}"}