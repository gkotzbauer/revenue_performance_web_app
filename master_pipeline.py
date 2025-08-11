#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
master_pipeline.py — Orchestrates the full Revenue Performance pipeline.

Place this file in your repo at: backend/master_pipeline.py

Order of operations:
  0) preprocess_invoice_data.py
  1) enhance_invoice_metrics.py
  2) generate_weekly_outputs.py
  2.1) build_diagnostics_base.py                (consumes Step 2 agg)
  2.2) build_ml_rate_diagnostics_boosted.py     (preferred ML; consumes Step 2 agg)  [optional]
  2.5) build_ml_rate_diagnostics.py             (ElasticNet baseline; optional)
  3) build_underpayment_summary.py              (quick totals)
  4) build_underpayment_drivers.py              (payer / key / time)
  5) build_cpt_rate_drivers.py                  (CPT-set drivers vs 85% E/M)
  6) revenue_performance_model.py               (consumes diagnostics base)
  7) final_narrative_module.py                  (ML-aware; prefers *_ml_boosted.csv)
  8) validate_invoice_sample.py                 (deterministic sample)        [optional]
  8b) validate_invoice_sample_random.py         (random sample validator)     [optional]

Environment variables you can tune without editing code:
  MATERIALITY_PCT       -> weighting/ML thresholds (default: 0.03 = 3%)
  PERF_OVER_PCT         -> +performance band (default: 0.05)
  PERF_UNDER_PCT        -> -performance band (default: -0.05)
  HGB_MATERIALITY_PER_VISIT -> $/visit threshold for boosted ML gaps (default: 10)
  DATA_DIR              -> working data directory (default: /mnt/data)

Usage:
  python backend/master_pipeline.py
  python backend/master_pipeline.py "Step 3"   # start at a given step (prefix match)

All scripts are expected to be in the same folder as this file (backend/).
"""

import os
import sys
import subprocess
from pathlib import Path

# -----------------------------
# Config
# -----------------------------
HERE = Path(__file__).parent
DATA_DIR = Path(os.getenv("DATA_DIR", "/mnt/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Ordered pipeline
SCRIPTS = [
    ("Step 0: Preprocess Invoice-Level Data",          "preprocess_invoice_data.py",          False),
    ("Step 1: Enhance Invoice Data w/ Benchmarks",     "enhance_invoice_metrics.py",          False),
    ("Step 2: Weekly Outputs (granular + agg)",        "generate_weekly_outputs.py",          False),
    ("Step 2.1: Diagnostics Base (85% & Benchmark)",   "build_diagnostics_base.py",           False),
    ("Step 2.2: ML Rate Diagnostics (HGB, preferred)", "build_ml_rate_diagnostics_boosted.py",True),  # optional
    ("Step 2.5: ML Rate Diagnostics (ElasticNet)",     "build_ml_rate_diagnostics.py",        True),  # optional
    ("Step 3: Underpayment Summary (totals)",          "build_underpayment_summary.py",       False),
    ("Step 4: Underpayment Drivers (payer/key/time)",  "build_underpayment_drivers.py",       False),
    ("Step 5: CPT Rate Drivers vs 85% E/M",            "build_cpt_rate_drivers.py",           False),
    ("Step 6: Revenue Performance Summary (Base)",     "revenue_performance_model.py",        False),
    ("Step 7: Diagnostic Narratives (ML-aware)",       "final_narrative_module.py",           False),
    ("Step 8: Sample-Based Validation",                "validate_invoice_sample.py",          True),  # optional
    ("Step 8b: Sample-Based Validation (random)",      "validate_invoice_sample_random.py",   True),  # optional
]

# Optional: allow command-line filter to run from a given step name (prefix match)
START_AT = None
if len(sys.argv) > 1:
    START_AT = sys.argv[1].strip().lower()

def should_run(step_name: str) -> bool:
    if START_AT is None:
        return True
    return step_name.strip().lower().startswith(START_AT)

def run_script(label: str, script: str, optional: bool, extra_env=None):
    script_path = HERE / script
    if not script_path.is_file():
        if optional:
            print(f"⚠️  {label}: missing {script} — skipping (optional).")
            return
        raise FileNotFoundError(f"Missing script: {script_path}")
    print(f"\n▶ {label} — running: {script}")
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    # Ensure outputs land under DATA_DIR if scripts honor this var
    env.setdefault("DATA_DIR", str(DATA_DIR))
    subprocess.run([sys.executable, str(script_path)], check=True, env=env, cwd=str(HERE))
    print(f"✅ {label} — completed.")

def main():
    # Let users tune thresholds without editing code
    env_overrides = {
        "MATERIALITY_PCT": os.getenv("MATERIALITY_PCT", "0.03"),   # 3%
        "PERF_OVER_PCT": os.getenv("PERF_OVER_PCT", "0.05"),       # +5%
        "PERF_UNDER_PCT": os.getenv("PERF_UNDER_PCT", "-0.05"),    # -5%
        "HGB_MATERIALITY_PER_VISIT": os.getenv("HGB_MATERIALITY_PER_VISIT", "10"),  # $10/visit
    }

    for label, script, optional in SCRIPTS:
        if should_run(label):
            run_script(label, script, optional=optional, extra_env=env_overrides)
        else:
            print(f"⏭  Skipping {label} (will start at '{START_AT}')")

    print("\n🎉 All pipeline steps completed successfully.")

if __name__ == "__main__":
    main()
