# backend/app.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from fastapi.exceptions import HTTPException
import os

from .routes.upload_route import router as upload_router
from .routes.pipeline_route import router as pipeline_router
from .routes.download_route import router as download_router
from .routes.logs_route import router as logs_router
from .routes.ml_results_route import router as ml_router

# ---------------------------------------
# App & CORS
# ---------------------------------------
app = FastAPI(
    title="Revenue Performance Web App",
    version="1.0.0",
    description="Upload source data, run the revenue analytics pipeline, and download outputs."
)

# Allow local dev & Render origins (adjust as needed)
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://*.onrender.com",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# ---------------------------------------
# Static mounts (expose outputs if desired)
# ---------------------------------------
ROOT = Path(__file__).resolve().parents[1]  # repo root /backend -> parent is project root
DATA_DIR = ROOT / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
OUTPUTS_DIR = DATA_DIR / "outputs"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# Optional: serve outputs so downloads can be direct links
app.mount("/outputs", StaticFiles(directory=str(OUTPUTS_DIR)), name="outputs")

# ---------------------------------------
# Routers
# ---------------------------------------
app.include_router(upload_router)
app.include_router(pipeline_router)
app.include_router(download_router)
app.include_router(logs_router)
app.include_router(ml_router)

# ---------------------------------------
# Static file serving for React frontend
# ---------------------------------------
STATIC_DIR = Path(__file__).parent / "static"

@app.middleware("http")
async def static_files_middleware(request: Request, call_next):
    # Skip API routes
    if request.url.path.startswith("/api/"):
        return await call_next(request)
    
    # Skip outputs route
    if request.url.path.startswith("/outputs/"):
        return await call_next(request)
    
    # Handle static files
    if STATIC_DIR.exists():
        # Try to serve the requested file
        file_path = STATIC_DIR / request.url.path.lstrip("/")
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        
        # For root or non-existent files, serve index.html for SPA routing
        if request.url.path == "/" or not file_path.exists():
            index_path = STATIC_DIR / "index.html"
            if index_path.exists():
                return FileResponse(str(index_path))
    
    # If no static file found, continue with normal request processing
    return await call_next(request)

# ---------------------------------------
# Health
# ---------------------------------------
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "message": "Revenue Performance API is up.",
        "uploads_dir": str(UPLOADS_DIR),
        "outputs_dir": str(OUTPUTS_DIR),
    }

# ---------------------------------------
# Local run
# ---------------------------------------
# Run with: uvicorn backend.app:app --reload --port 8000
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.app:app", host="0.0.0.0", port=8000, reload=True)
