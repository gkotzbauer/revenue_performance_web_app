import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from .file_utils import (
    load_csv_file,
    load_excel_file,
    normalize_to_csv_or_excel,
)


ValidationReport = Dict[str, Union[str, bool, List[str], Dict[str, int]]]


# ============================================================
# Core helpers
# ============================================================
def _ok(msg: str = "OK") -> ValidationReport:
    return {"status": "ok", "passed": True, "issues": [], "summary": msg}


def _fail(issues: List[str], summary: str = "Validation failed") -> ValidationReport:
    return {"status": "fail", "passed": False, "issues": issues, "summary": summary}


def _warn(issues: List[str], summary: str = "Validation warnings") -> ValidationReport:
    return {"status": "warn", "passed": True, "issues": issues, "summary": summary}


def _load_df(path: Union[str, Path]) -> pd.DataFrame:
    p = Path(path)
    p, kind = normalize_to_csv_or_excel(p)
    if kind == "csv":
        return load_csv_file(p)
    return load_excel_file(p)


# ============================================================
# File-level checks
# ============================================================
def check_file_exists(path: Union[str, Path]) -> ValidationReport:
    p = Path(path)
    if p.is_file():
        return _ok(f"Found file: {p.name}")
    return _fail([f"Missing file: {p}"])


def check_non_empty_file(path: Union[str, Path]) -> ValidationReport:
    p = Path(path)
    if not p.is_file():
        return _fail([f"Missing file: {p}"])
    if p.stat().st_size <= 0:
        return _fail([f"File is empty: {p.name}"])
    return _ok(f"File size: {p.stat().st_size} bytes")


# ============================================================
# DataFrame-level schema checks
# ============================================================
def check_columns_present(
    df: pd.DataFrame,
    required_cols: List[str],
    allow_any_subset: bool = False
) -> ValidationReport:
    cols = set(df.columns)
    missing = [c for c in required_cols if c not in cols]
    if not missing:
        return _ok("All required columns present")
    if allow_any_subset and len(missing) < len(required_cols):
        return _warn([f"Missing columns (partial ok): {missing}"], "Some columns missing")
    return _fail([f"Missing required columns: {missing}"])


def check_no_all_null_columns(
    df: pd.DataFrame,
    critical_cols: Optional[List[str]] = None
) -> ValidationReport:
    issues = []
    cols = critical_cols if critical_cols else list(df.columns)
    for c in cols:
        if c in df.columns and df[c].isna().all():
            issues.append(f"Column '{c}' is entirely null")
    if issues:
        return _warn(issues, "Found columns that are entirely null")
    return _ok("No all-null columns among checked set")


def check_row_count(
    df: pd.DataFrame,
    min_rows: int = 1
) -> ValidationReport:
    n = len(df)
    if n >= min_rows:
        return _ok(f"Row count OK: {n} (>= {min_rows})")
    return _fail([f"Too few rows: {n} < {min_rows}"])


def check_unique_keys(
    df: pd.DataFrame,
    key_cols: List[str]
) -> ValidationReport:
    if not set(key_cols).issubset(df.columns):
        return _fail([f"Key columns missing for uniqueness check: {key_cols}"])
    dups = df.duplicated(subset=key_cols, keep=False).sum()
    if dups == 0:
        return _ok("Key uniqueness OK")
    return _warn([f"Found {dups} duplicate rows by keys {key_cols}"], "Duplicates detected")


def basic_numeric_sanity(
    df: pd.DataFrame,
    numeric_cols: List[str],
    non_negative: bool = True,
    finite_only: bool = True
) -> ValidationReport:
    issues = []
    for c in numeric_cols:
        if c not in df.columns:
            issues.append(f"Numeric column missing: {c}")
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if finite_only:
            bad = (~np.isfinite(s)).sum()
            if bad > 0:
                issues.append(f"{c}: {bad} non-finite values")
        if non_negative:
            neg = (s < 0).sum()
            if neg > 0:
                issues.append(f"{c}: {neg} negative values (expected >= 0)")
    if issues:
        return _warn(issues, "Numeric sanity warnings")
    return _ok("Numeric sanity OK")


# ============================================================
# Step-by-step validators (call after each pipeline step)
# ============================================================
def validate_after_preprocess(output_csv: Union[str, Path]) -> ValidationReport:
    # Expect cleaned invoice-level CSV
    fcheck = check_file_exists(output_csv)
    if not fcheck["passed"]:
        return fcheck

    try:
        df = _load_df(output_csv)
    except Exception as e:
        return _fail([f"Failed to load preprocess output: {e}"])

    schema = [
        "Invoice_Number", "Year", "Week", "Payer",
        "Group_EM", "Group_EM2", "Charge CPT Code"
    ]
    r = check_columns_present(df, schema)
    if not r["passed"]:
        return r

    r2 = check_row_count(df, min_rows=10)
    if not r2["passed"]:
        return r2

    # basic uniqueness by invoice + CPT row
    r3 = check_unique_keys(df, ["Invoice_Number", "Charge CPT Code"])
    # allow warn
    return r3 if r3["status"] != "ok" else _ok("Preprocess validation passed")


def validate_after_enhance(output_csv: Union[str, Path]) -> ValidationReport:
    fcheck = check_file_exists(output_csv)
    if not fcheck["passed"]:
        return fcheck
    try:
        df = _load_df(output_csv)
    except Exception as e:
        return _fail([f"Failed to load enhanced output: {e}"])

    required = [
        "Invoice_Number", "Payer", "Group_EM", "Group_EM2", "Benchmark_Key",
        "Payment Amount*", "Expected Amount (85% E/M)"
    ]
    r = check_columns_present(df, required)
    if not r["passed"]:
        return r

    # Numeric sanity
    num = ["Payment Amount*", "Expected Amount (85% E/M)"]
    r2 = basic_numeric_sanity(df, num, non_negative=True, finite_only=True)
    if r2["status"] == "fail":
        return r2

    return _ok("Enhance validation passed")


def validate_after_weekly_outputs(
    granular_csv: Union[str, Path],
    agg_csv: Union[str, Path]
) -> ValidationReport:
    issues = []

    for path, tag in [(granular_csv, "granular"), (agg_csv, "agg")]:
        fcheck = check_file_exists(path)
        if not fcheck["passed"]:
            issues.append(f"[{tag}] {fcheck['issues'][0]}")
            continue
        try:
            df = _load_df(path)
        except Exception as e:
            issues.append(f"[{tag}] Failed to load: {e}")
            continue

        base_cols = ["Year", "Week", "Payer", "Group_EM", "Group_EM2",
                     "Visit_Count", "Payment_Amount"]
        r = check_columns_present(df, base_cols)
        if not r["passed"]:
            issues.append(f"[{tag}] {r['issues'][0]}")

        r2 = check_row_count(df, min_rows=5)
        if not r2["passed"]:
            issues.append(f"[{tag}] {r2['issues'][0]}")

    if issues:
        return _warn(issues, "Weekly outputs present with warnings")
    return _ok("Weekly outputs validation passed")


def validate_after_diagnostics_base(path: Union[str, Path]) -> ValidationReport:
    fcheck = check_file_exists(path)
    if not fcheck["passed"]:
        return fcheck

    try:
        df = _load_df(path)
    except Exception as e:
        return _fail([f"Failed to load diagnostics base: {e}"])

    required = [
        "Year", "Week", "Payer", "Group_EM", "Group_EM2",
        "Payment_Amount", "Expected_Payment",
        "Revenue_Variance_vs_85EM_$", "Revenue_Variance_vs_85EM_%",
        "Revenue_Variance_vs_Benchmark_$", "Revenue_Variance_vs_Benchmark_%"
    ]
    r = check_columns_present(df, required, allow_any_subset=True)
    if not r["passed"]:
        return r

    return _ok("Diagnostics base validation passed")


def validate_after_ml_agg(path: Union[str, Path]) -> ValidationReport:
    fcheck = check_file_exists(path)
    if not fcheck["passed"]:
        return fcheck
    try:
        df = _load_df(path)
    except Exception as e:
        return _fail([f"Failed to load ML agg: {e}"])

    required = [
        "Year", "Week", "Payer", "Group_EM", "Group_EM2",
        # HGB (preferred) or ElasticNet fields — we accept either set
        # Check presence loosely
    ]
    r = check_columns_present(df, required)
    if not r["passed"]:
        return r

    ml_cols_hgb = {"HGB_Expected_Rate_per_Visit", "HGB_Rate_Gap", "HGB_Dollar_Gap"}
    ml_cols_elastic = {"ML_Expected_Rate_per_Visit", "ML_Rate_Gap", "ML_Dollar_Gap"}

    if not (ml_cols_hgb.issubset(df.columns) or ml_cols_elastic.issubset(df.columns)):
        return _warn(
            [f"ML columns not found (neither HGB nor ElasticNet set)."],
            "ML diagnostics unavailable"
        )

    return _ok("ML agg validation passed")


def validate_after_narratives(out_xlsx: Union[str, Path]) -> ValidationReport:
    # Existence only (Excel parse may fail if engine is missing in runtime;
    # narration is generated earlier).
    fcheck = check_file_exists(out_xlsx)
    if not fcheck["passed"]:
        return fcheck
    return _ok("Narratives exported")


def validate_after_underpayment_outputs(
    summary_csv: Union[str, Path],
    drivers_key_csv: Union[str, Path],
    drivers_payer_csv: Union[str, Path]
) -> ValidationReport:
    issues = []
    for path, tag in [(summary_csv, "summary"), (drivers_key_csv, "by_key"), (drivers_payer_csv, "by_payer")]:
        fcheck = check_file_exists(path)
        if not fcheck["passed"]:
            issues.append(f"[{tag}] {fcheck['issues'][0]}")
    if issues:
        return _fail(issues, "Underpayment outputs missing")
    return _ok("Underpayment outputs present")


# ============================================================
# Composite runner for UI checkpoints
# ============================================================
def run_checkpoint(
    label: str,
    file_path: Union[str, Path],
    kind: str,
    extras: Optional[Dict[str, Union[str, Path]]] = None
) -> ValidationReport:
    """
    Generic wrapper the API can call. `kind` controls which validator runs.
      kind ∈ {
        "preprocess", "enhance", "weekly", "diag_base",
        "ml_agg", "narratives", "underpay"
      }
    `extras` allows passing additional paths (e.g., granular+agg).
    """
    try:
        if kind == "preprocess":
            return validate_after_preprocess(file_path)
        if kind == "enhance":
            return validate_after_enhance(file_path)
        if kind == "weekly":
            if not extras or "agg_csv" not in extras:
                return _fail(["Missing extras['agg_csv'] for weekly validation"])
            return validate_after_weekly_outputs(file_path, extras["agg_csv"])
        if kind == "diag_base":
            return validate_after_diagnostics_base(file_path)
        if kind == "ml_agg":
            return validate_after_ml_agg(file_path)
        if kind == "narratives":
            return validate_after_narratives(file_path)
        if kind == "underpay":
            if not extras or not all(k in extras for k in ("drivers_key_csv", "drivers_payer_csv")):
                return _fail(["Missing extras for underpayment validation"])
            return validate_after_underpayment_outputs(file_path, extras["drivers_key_csv"], extras["drivers_payer_csv"])
        return _fail([f"Unknown checkpoint kind: {kind}"])
    except Exception as e:
        return _fail([f"{label} checkpoint error: {e}"], "Exception during validation")
