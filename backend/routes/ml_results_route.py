# backend/routes/ml_results_route.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from pathlib import Path
import os

from ..utils.file_utils import ensure_dirs

router = APIRouter(prefix="/api/ml-results", tags=["ml-results"])

# Define constants locally since they're not in pipeline_utils
PIPELINE_ROOT = Path(__file__).resolve().parents[2]  # repo root
OUTPUTS_DIR = PIPELINE_ROOT / "data" / "outputs"

ensure_dirs()

# Preferred order to read ML-enhanced aggregated outputs
ML_CANDIDATE_PATTERNS = [
    "*_ml_boosted.csv",                   # produced by build_ml_rate_diagnostics_boosted.py
    "v2_Rev_Perf_Weekly_Model_Output_Final_agg_ml.csv",  # produced by build_ml_rate_diagnostics.py
    "*_agg_ml.csv",
]

REQUIRED_KEYS = ["Year", "Week", "Payer", "Group_EM", "Group_EM2"]


def _to_float_safe(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan


def _load_latest_ml_file() -> Path:
    files: List[Path] = []
    for pat in ML_CANDIDATE_PATTERNS:
        files.extend(OUTPUTS_DIR.glob(pat))
    if not files:
        # As a fallback, try the base aggregated file (won't have ML cols)
        fallback = list(OUTPUTS_DIR.glob("v2_Rev_Perf_Weekly_Model_Output_Final_agg*"))
        if not fallback:
            raise FileNotFoundError("No ML or aggregated files found under /data/outputs.")
        return sorted(fallback, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _load_df(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)


def _ensure_group_keys(df: pd.DataFrame):
    for k in REQUIRED_KEYS:
        if k not in df.columns:
            raise HTTPException(status_code=400, detail=f"Missing required column '{k}' in ML output.")


def _normalize_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(_to_float_safe)
    return df


@router.get("/summary", response_class=JSONResponse)
async def ml_summary() -> Dict[str, Any]:
    """
    High-level ML diagnostics summary:
      - Total ML_Dollar_Gap, count of material flags
      - Top under/over-performing groups by ML_Dollar_Gap
      - File metadata
    Works with either boosted or elasticnet ML outputs.
    """
    ml_path = _load_latest_ml_file()
    df = _load_df(ml_path)
    _ensure_group_keys(df)

    # Normalize likely numeric ML columns (support both boosted & elasticnet variants)
    numeric_candidates = [
        "ML_Expected_Rate_per_Visit", "ML_Rate_Gap", "ML_Dollar_Gap",
        "ML_Material_Gap_Flag",
        "HGB_Expected_Rate_per_Visit", "HGB_Rate_Gap", "HGB_Dollar_Gap", "HGB_Material_Gap_Flag",
        "Visit_Count", "Payment_Amount"
    ]
    df = _normalize_numeric(df, numeric_candidates)

    # Prefer HGB columns if present
    has_hgb = {"HGB_Dollar_Gap", "HGB_Material_Gap_Flag"}.issubset(df.columns)
    dollar_col = "HGB_Dollar_Gap" if has_hgb else ("ML_Dollar_Gap" if "ML_Dollar_Gap" in df.columns else None)
    flag_col = "HGB_Material_Gap_Flag" if has_hgb else ("ML_Material_Gap_Flag" if "ML_Material_Gap_Flag" in df.columns else None)

    if dollar_col is None:
        return {
            "status": "ok",
            "message": "No ML gap columns detected. Did you run the ML diagnostics step?",
            "file": str(ml_path),
            "has_hgb": has_hgb
        }

    # Aggregate by payer/E&M to provide compact summary
    grp_cols = ["Year", "Week", "Payer", "Group_EM", "Group_EM2"]
    agg = (
        df.groupby(grp_cols, dropna=False)
          .agg(
              Dollar_Gap_Sum=(dollar_col, "sum"),
              Material_Flags=(flag_col, "sum") if flag_col in df.columns else (dollar_col, "count"),
          )
          .reset_index()
    )

    # Top 10 under/over by dollar gap
    top_under = agg.nsmallest(10, "Dollar_Gap_Sum").to_dict(orient="records")
    top_over  = agg.nlargest(10, "Dollar_Gap_Sum").to_dict(orient="records")

    return {
        "status": "ok",
        "file": str(ml_path),
        "has_hgb": has_hgb,
        "totals": {
            "Dollar_Gap_Total": float(np.nansum(df[dollar_col])),
            "Material_Flags_Total": int(np.nansum(df[flag_col])) if flag_col in df.columns else None
        },
        "top_under": top_under,
        "top_over": top_over
    }


@router.get("/hotspots", response_class=JSONResponse)
async def ml_hotspots(
    top_n: int = Query(20, ge=1, le=200, description="Number of hotspots to return")
) -> Dict[str, Any]:
    """
    Return top-N hotspots sorted by absolute dollar gap (|gap|), including
    group keys and basic context.
    """
    ml_path = _load_latest_ml_file()
    df = _load_df(ml_path)
    _ensure_group_keys(df)

    numeric_candidates = [
        "HGB_Dollar_Gap", "HGB_Rate_Gap", "HGB_Expected_Rate_per_Visit",
        "ML_Dollar_Gap", "ML_Rate_Gap", "ML_Expected_Rate_per_Visit",
        "Visit_Count", "Payment_Amount"
    ]
    df = _normalize_numeric(df, numeric_candidates)

    dollar_col = "HGB_Dollar_Gap" if "HGB_Dollar_Gap" in df.columns else "ML_Dollar_Gap"
    if dollar_col not in df.columns:
        raise HTTPException(status_code=400, detail="No ML dollar gap column found in file.")

    df["_abs_gap"] = df[dollar_col].abs()
    cols = ["Year", "Week", "Payer", "Group_EM", "Group_EM2", dollar_col]
    # Prefer to summarize at the grouping level:
    grp = (
        df.groupby(["Year", "Week", "Payer", "Group_EM", "Group_EM2"], dropna=False)[[dollar_col]]
          .sum()
          .reset_index()
    )
    grp["_abs_gap"] = grp[dollar_col].abs()
    out = grp.sort_values("_abs_gap", ascending=False).head(top_n)
    out = out.drop(columns="_abs_gap")
    return {
        "status": "ok",
        "file": str(ml_path),
        "dollar_col": dollar_col,
        "hotspots": out.to_dict(orient="records")
    }


@router.get("/record", response_class=JSONResponse)
async def ml_record(
    year: int,
    week: int,
    payer: str,
    group_em: str,
    group_em2: str
) -> Dict[str, Any]:
    """
    Return the ML fields for a specific (Year, Week, Payer, Group_EM, Group_EM2) record.
    """
    ml_path = _load_latest_ml_file()
    df = _load_df(ml_path)
    _ensure_group_keys(df)

    mask = (
        (df["Year"].astype(str) == str(year)) &
        (df["Week"].astype(str) == str(week)) &
        (df["Payer"].astype(str) == payer) &
        (df["Group_EM"].astype(str) == group_em) &
        (df["Group_EM2"].astype(str) == group_em2)
    )

    row = df.loc[mask]
    if row.empty:
        raise HTTPException(status_code=404, detail="Record not found.")

    # Pick common ML columns (both boosted and elasticnet)
    cols = [
        "Visit_Count", "Payment_Amount",
        "HGB_Expected_Rate_per_Visit", "HGB_Rate_Gap", "HGB_Dollar_Gap", "HGB_Material_Gap_Flag",
        "ML_Expected_Rate_per_Visit",  "ML_Rate_Gap",  "ML_Dollar_Gap",  "ML_Material_Gap_Flag",
    ]
    present_cols = [c for c in cols if c in row.columns]
    # Coerce numerics for cleanliness
    for c in present_cols:
        row[c] = row[c].apply(_to_float_safe)

    return {
        "status": "ok",
        "file": str(ml_path),
        "row": row[present_cols + REQUIRED_KEYS].to_dict(orient="records")[0]
    }
