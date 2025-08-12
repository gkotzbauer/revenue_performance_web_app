# backend/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from fastapi.exceptions import HTTPException

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

# Serve the React frontend build
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    # Mount the React app at root - API routes mounted earlier will take precedence
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

# ---------------------------------------
# Routers
# ---------------------------------------
app.include_router(upload_router)
app.include_router(pipeline_router)
app.include_router(download_router)
app.include_router(logs_router)
app.include_router(ml_router)

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
