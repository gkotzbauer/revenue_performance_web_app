#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Master pipeline orchestrator for the Revenue Performance web app.
- Runs each Python step in order (skipping optional steps if missing).
- Passes common ENV vars (DATA_DIR, UPLOADS_DIR, OUTPUTS_DIR, LOGS_DIR, thresholds).
- Writes a lightweight artifact manifest at data/outputs/_ARTIFACTS.json

This script prints logs to STDOUT; the FastAPI backend captures and exposes them.
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------
# Paths & common dirs
# -----------------------------------------------------------
ROOT_DIR     = Path(__file__).parent.resolve()
DATA_DIR     = Path(os.getenv("DATA_DIR", ROOT_DIR / "data")).resolve()
UPLOADS_DIR  = Path(os.getenv("UPLOADS_DIR", DATA_DIR / "uploads")).resolve()
OUTPUTS_DIR  = Path(os.getenv("OUTPUTS_DIR", DATA_DIR / "outputs")).resolve()
LOGS_DIR     = Path(os.getenv("LOGS_DIR", ROOT_DIR / "logs")).resolve()
ARTIFACTS    = OUTPUTS_DIR / "_ARTIFACTS.json"

for p in (DATA_DIR, UPLOADS_DIR, OUTPUTS_DIR, LOGS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------
# Ordered pipeline steps (adjust names if your script files differ)
# -----------------------------------------------------------
SCRIPTS = [
    ("Step 0: Preprocess Invoice-Level Data",          "preprocess_invoice_data.py",          True),
    ("Step 1: Enhance Invoice Data w/ Benchmarks",     "enhance_invoice_metrics.py",          True),
    ("Step 2: Weekly Outputs (granular + agg)",        "generate_weekly_outputs.py",          True),
    ("Step 2.1: Diagnostics Base (85% & Benchmark)",   "build_diagnostics_base.py",           True),
    ("Step 2.5: ML Rate Diagnostics (ElasticNet)",     "build_ml_rate_diagnostics.py",        False),
    ("Step 2.6: ML Rate Diagnostics (HGB Boosted)",    "build_ml_rate_diagnostics_boosted.py",False),
    ("Step 3: Underpayment Summary (totals)",          "build_underpayment_summary.py",       True),
    ("Step 4: Underpayment Drivers (payer/key/time)",  "build_underpayment_drivers.py",       True),
    ("Step 5: CPT Rate Drivers vs 85% E/M",            "build_cpt_rate_drivers.py",           True),
    ("Step 6: Revenue Performance Summary (Base)",     "revenue_performance_model.py",        True),
    ("Step 7: Diagnostic Narratives (ML-aware)",       "final_narrative_module.py",           True),
    ("Step 8: Sample-Based Validation",                "validate_invoice_sample.py",          False),
    ("Step 8b: Random Sample Validation",              "validate_invoice_sample_random.py",   False),
]

# -----------------------------------------------------------
# Thresholds / knobs (can be overridden by environment)
# -----------------------------------------------------------
ENV_OVERRIDES = {
    "MATERIALITY_PCT": os.getenv("MATERIALITY_PCT", "0.03"),
    "PERF_OVER_PCT":   os.getenv("PERF_OVER_PCT", "0.05"),
    "PERF_UNDER_PCT":  os.getenv("PERF_UNDER_PCT", "-0.05"),
    # Per-visit materiality for ML flags (ElasticNet/HGB)
    "ML_MATERIALITY_PER_VISIT": os.getenv("ML_MATERIALITY_PER_VISIT", "10"),
    # Make sure pandas/np prints are deterministic
    "PYTHONHASHSEED": os.getenv("PYTHONHASHSEED", "0"),
}

def run_step(label: str, script_name: str, required: bool) -> None:
    """
    Run a single pipeline script with inherited + override environment.
    Raises on failure if required=True; otherwise logs and continues.
    """
    script_path = ROOT_DIR / script_name
    print(f"\n▶ {label} — running: {script_name}")

    if not script_path.exists():
        if required:
            raise FileNotFoundError(f"Missing required script: {script_name}")
        print(f"  ⚠️ Optional script missing, skipping: {script_name}")
        return

    env = os.environ.copy()
    env.update(ENV_OVERRIDES)
    env["DATA_DIR"]    = str(DATA_DIR)
    env["UPLOADS_DIR"] = str(UPLOADS_DIR)
    env["OUTPUTS_DIR"] = str(OUTPUTS_DIR)
    env["LOGS_DIR"]    = str(LOGS_DIR)

    # Run the script and stream its stdout/stderr to this process
    proc = subprocess.run([sys.executable, str(script_path)], cwd=str(ROOT_DIR), env=env)
    if proc.returncode != 0:
        msg = f"❌ {label} failed with exit code {proc.returncode}"
        if required:
            raise RuntimeError(msg)
        print(f"  ⚠️ {msg} (optional step; continuing)")
    else:
        print(f"✅ {label} — completed successfully.")

def summarize_artifacts() -> None:
    """
    Write a simple manifest of what's in data/outputs/ so the UI can present links.
    """
    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "outputs_dir": str(OUTPUTS_DIR),
        "files": sorted([p.name for p in OUTPUTS_DIR.glob("*") if p.is_file()]),
    }
    ARTIFACTS.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\n🧾 Wrote artifact manifest: {ARTIFACTS}")

def main():
    print("🚀 Starting Revenue Performance Pipeline")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"DATA_DIR   = {DATA_DIR}")
    print(f"UPLOADS_DIR= {UPLOADS_DIR}")
    print(f"OUTPUTS_DIR= {OUTPUTS_DIR}")
    print(f"LOGS_DIR   = {LOGS_DIR}")

    # Run steps
    for label, script, required in SCRIPTS:
        run_step(label, script, required)

    # Summarize outputs
    summarize_artifacts()
    print("\n🎉 All pipeline steps completed.")

if __name__ == "__main__":
    main()