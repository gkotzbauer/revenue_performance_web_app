import os
import re
import io
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple, Union

import pandas as pd


# =========================================
# Constants & Directories
# =========================================
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../revenue_performance_web_app
DATA_DIR = ROOT_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
OUTPUTS_DIR = DATA_DIR / "outputs"

DEFAULT_ALLOWED_EXTS = {".csv", ".xlsx", ".xls", ".zip"}


# =========================================
# Directory helpers
# =========================================
def ensure_dirs() -> None:
    """Ensure expected data directories exist."""
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


# =========================================
# Filename & extension helpers
# =========================================
def secure_filename(filename: str) -> str:
    """
    Very small 'secure_filename' implementation:
    - strips directory components
    - keeps alnum, dash, underscore, dot
    - collapses spaces
    """
    filename = os.path.basename(filename)
    filename = filename.strip().replace(" ", "_")
    # allow only safe characters
    filename = re.sub(r"[^A-Za-z0-9._-]", "", filename)
    # prevent hidden files like ".env"
    if filename.startswith("."):
        filename = filename[1:]
    return filename or f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def allowed_file(filename: str, allowed_exts: Optional[set] = None) -> bool:
    """Check if file extension is allowed."""
    allowed_exts = allowed_exts or DEFAULT_ALLOWED_EXTS
    suffix = Path(filename).suffix.lower()
    return suffix in allowed_exts


# =========================================
# Saving uploads
# =========================================
def save_uploaded_file(
    file_obj: Union[io.BytesIO, "FileStorage", bytes, str, Path],
    dest_dir: Path = UPLOADS_DIR,
    allowed_exts: Optional[set] = None
) -> Path:
    """
    Save different file-like inputs into dest_dir.
    Supports:
      - Flask/Django style 'FileStorage' with .filename and .save()
      - bytes / BytesIO
      - local path string / Path (will copy)
    Returns the destination Path.
    """
    ensure_dirs()
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Case A: Werkzeug/Django style object (has .filename and .save)
    if hasattr(file_obj, "filename") and hasattr(file_obj, "save"):
        filename = secure_filename(file_obj.filename)
        if not allowed_file(filename, allowed_exts):
            raise ValueError(f"Disallowed file type: {filename}")
        dest_path = dest_dir / filename
        file_obj.save(dest_path)
        return dest_path

    # Case B: bytes-like
    if isinstance(file_obj, (bytes, bytearray, io.BytesIO)):
        # Caller should specify a name externally; otherwise timestamp it
        filename = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bin"
        dest_path = dest_dir / filename
        with open(dest_path, "wb") as f:
            if isinstance(file_obj, io.BytesIO):
                f.write(file_obj.getvalue())
            else:
                f.write(file_obj)
        return dest_path

    # Case C: local path (copy)
    src = Path(file_obj)
    if src.is_file():
        filename = secure_filename(src.name)
        if not allowed_file(filename, allowed_exts):
            raise ValueError(f"Disallowed file type: {filename}")
        dest_path = dest_dir / filename
        shutil.copy2(src, dest_path)
        return dest_path

    raise TypeError("Unsupported file_obj type for save_uploaded_file().")


# =========================================
# Loaders
# =========================================
def load_csv_file(path: Union[str, Path], **read_csv_kwargs) -> pd.DataFrame:
    """Load CSV with sane defaults, raising if missing."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"CSV not found: {p}")
    return pd.read_csv(p, **read_csv_kwargs)


def load_excel_file(path: Union[str, Path], sheet_name=0, **read_excel_kwargs) -> pd.DataFrame:
    """Load Excel with sane defaults, raising if missing."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Excel not found: {p}")
    return pd.read_excel(p, sheet_name=sheet_name, **read_excel_kwargs)


# =========================================
# Output helpers
# =========================================
def write_dataframe_csv(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame to outputs dir (CSV) and return path."""
    ensure_dirs()
    filename = secure_filename(filename)
    if not filename.endswith(".csv"):
        filename += ".csv"
    dest = OUTPUTS_DIR / filename
    df.to_csv(dest, index=False)
    return dest


def write_dataframe_excel(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame to outputs dir (XLSX) and return path."""
    ensure_dirs()
    filename = secure_filename(filename)
    if not filename.endswith(".xlsx"):
        filename += ".xlsx"
    dest = OUTPUTS_DIR / filename
    # Avoid Excel engine issues in minimal environments
    try:
        df.to_excel(dest, index=False)
    except Exception:
        # Fallback: write CSV with XLSX extension if Excel lib unavailable
        df.to_csv(dest.with_suffix(".csv"), index=False)
        return dest.with_suffix(".csv")
    return dest


def list_outputs(patterns: Optional[List[str]] = None) -> List[Path]:
    """List output files matching any of the patterns; if None, return all files."""
    ensure_dirs()
    if not patterns:
        return sorted([p for p in OUTPUTS_DIR.glob("*") if p.is_file()])
    results = []
    for pat in patterns:
        results.extend(OUTPUTS_DIR.glob(pat))
    # unique & sorted
    return sorted(set([p for p in results if p.is_file()]))


def get_latest_file(dir_path: Union[str, Path], pattern: str = "*") -> Optional[Path]:
    """Return the most recently modified file matching pattern, or None."""
    p = Path(dir_path)
    if not p.exists():
        return None
    files = [f for f in p.glob(pattern) if f.is_file()]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def get_latest_uploaded_file(pattern: str = "*") -> Optional[Path]:
    """Return latest uploaded file in uploads dir."""
    ensure_dirs()
    return get_latest_file(UPLOADS_DIR, pattern=pattern)


def get_latest_output_file(pattern: str = "*") -> Optional[Path]:
    """Return latest output file."""
    ensure_dirs()
    return get_latest_file(OUTPUTS_DIR, pattern=pattern)


# =========================================
# Convenience paths for pipeline steps
# =========================================
def outputs_path(*parts) -> Path:
    """Build a path under data/outputs."""
    ensure_dirs()
    return OUTPUTS_DIR.joinpath(*parts)


def uploads_path(*parts) -> Path:
    """Build a path under data/uploads."""
    ensure_dirs()
    return UPLOADS_DIR.joinpath(*parts)


# =========================================
# Small utilities
# =========================================
def normalize_to_csv_or_excel(path: Union[str, Path]) -> Tuple[Path, str]:
    """
    Given a path, return (Path, kind) where kind in {"csv","excel"} based on suffix.
    Raises on unsupported formats.
    """
    p = Path(path)
    suf = p.suffix.lower()
    if suf == ".csv":
        return p, "csv"
    if suf in {".xlsx", ".xls"}:
        return p, "excel"
    raise ValueError(f"Unsupported file type: {suf}")


def save_bytes_as_file(content: bytes, filename: str, dest_dir: Path = OUTPUTS_DIR) -> Path:
    """Save raw bytes to a file in outputs (useful for generated zips)."""
    ensure_dirs()
    filename = secure_filename(filename)
    dest = dest_dir / filename
    with open(dest, "wb") as f:
        f.write(content)
    return dest