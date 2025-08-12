# backend/routes/pipeline_route.py
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
from pathlib import Path
import os
import subprocess
import sys

from ..utils.pipeline_utils import run_pipeline  # provided in utils

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Define constants locally since they're not in pipeline_utils
PIPELINE_ROOT = Path(__file__).resolve().parents[2]  # repo root
LOG_DIR = PIPELINE_ROOT / "logs"
OUTPUTS_DIR = PIPELINE_ROOT / "data" / "outputs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


@router.post("/run", response_class=JSONResponse)
async def run_full_pipeline(
    start_at: Optional[str] = Query(None, description="Optional step label prefix to start from")
) -> Dict[str, Any]:
    """
    Run the revenue performance pipeline end-to-end, or from a specific step.
    Logs are written to logs/pipeline.log.
    """
    env = os.environ.copy()
    if start_at:
        env["START_AT"] = start_at

    try:
        # Run the main pipeline script directly
        pipeline_script = PIPELINE_ROOT / "run_pipeline.py"
        if not pipeline_script.exists():
            raise FileNotFoundError(f"Pipeline script not found: {pipeline_script}")
        
        # Set environment variables for the pipeline
        env["DATA_DIR"] = str(DATA_DIR)
        env["UPLOADS_DIR"] = str(UPLOADS_DIR)
        env["OUTPUTS_DIR"] = str(OUTPUTS_DIR)
        env["LOGS_DIR"] = str(LOG_DIR)
        
        # Run the pipeline script
        result = subprocess.run(
            [sys.executable, str(pipeline_script)],
            env=env,
            cwd=str(PIPELINE_ROOT),
            capture_output=True,
            text=True
        )
        
        rc = result.returncode
        
        # Log the output for debugging
        print("Pipeline stdout:", result.stdout)
        if result.stderr:
            print("Pipeline stderr:", result.stderr)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {e}") from e

    return {
        "status": "ok" if rc == 0 else "error",
        "return_code": rc,
        "log_path": str(LOG_DIR / "pipeline.log"),
        "message": f"Pipeline completed with return code {rc}"
    }


@router.get("/status", response_class=JSONResponse)
async def pipeline_status() -> Dict[str, Any]:
    """
    Basic placeholder endpoint for pipeline status checks.
    Can be expanded to track real-time execution in the future.
    """
    log_file = LOG_DIR / "pipeline.log"
    if not log_file.exists():
        return {"status": "idle", "message": "No pipeline log found."}
    return {
        "status": "complete",
        "log_path": str(log_file),
        "last_modified": int(log_file.stat().st_mtime),
    }