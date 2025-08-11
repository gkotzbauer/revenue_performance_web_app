#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 5: CPT Rate Drivers vs 85% E/M
-----------------------------------
Consumes the aggregated weekly model output (from Step 2).
Computes CPT-level rate & $ drivers vs the 85% E/M Expected Payment benchmark.

Goal:
- Identify where CPT-level payments are lower than expected
- Highlight top dollar impact CPTs by payer / week / E/M group
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path

# ---------------------------
# Locate data directories
# ---------------------------
OUTPUTS_DIR = Path(os.getenv("OUTPUTS_DIR", "data/outputs")).resolve()
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# Try to locate the aggregated weekly file from Step 2
agg_file = None
for f in OUTPUTS_DIR.glob("*_agg.csv"):
    agg_file = f
    break
if not agg_file:
    raise FileNotFoundError(f"No *_agg.csv file found in {OUTPUTS_DIR}")

print(f"📂 Using aggregated weekly file: {agg_file.name}")

# ---------------------------
# Load data
# ---------------------------
df = pd.read_csv(agg_file)

# Ensure expected columns exist
required_cols = [
    "Year", "Week", "Payer", "Group_EM", "Group_EM2", "Benchmark_Key",
    "Charge CPT Code",
    "Payment_Amount", "Visit_Count",
    "Expected_Amount_85_EM",  # benchmark rate per visit
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# ---------------------------
# Compute actual rate per visit and variance
# ---------------------------
df["Actual_Rate_per_Visit"] = df["Payment_Amount"] / df["Visit_Count"]
df["Rate_Variance_vs_85EM"] = df["Actual_Rate_per_Visit"] - df["Expected_Amount_85_EM"]
df["Dollar_Impact_vs_85EM"] = df["Rate_Variance_vs_85EM"] * df["Visit_Count"]

# Flag underpayments
df["Underpaid_vs_85EM"] = df["Rate_Variance_vs_85EM"] < 0

# ---------------------------
# Summarize at CPT level
# ---------------------------
cpt_summary = (
    df.groupby(
        ["Year", "Week", "Payer", "Group_EM", "Group_EM2", "Charge CPT Code"],
        dropna=False
    )
    .agg(
        Total_Visits=("Visit_Count", "sum"),
        Avg_Actual_Rate=("Actual_Rate_per_Visit", "mean"),
        Avg_Expected_Rate_85EM=("Expected_Amount_85_EM", "mean"),
        Total_Dollar_Impact_vs_85EM=("Dollar_Impact_vs_85EM", "sum"),
        Underpaid_Flag=("Underpaid_vs_85EM", "any")
    )
    .reset_index()
)

# ---------------------------
# Sort by largest negative dollar impact
# ---------------------------
cpt_summary = cpt_summary.sort_values("Total_Dollar_Impact_vs_85EM")

# ---------------------------
# Save outputs
# ---------------------------
out_path = OUTPUTS_DIR / "cpt_rate_drivers_vs_85EM.csv"
cpt_summary.to_csv(out_path, index=False)

print(f"✅ CPT rate drivers written to {out_path}")
print(f"   Rows: {len(cpt_summary)}")

# ---------------------------
# Optional: print top 5 worst CPT gaps
# ---------------------------
print("\n🔍 Top 5 CPTs with largest underpayment impact vs 85% E/M:")
print(
    cpt_summary[cpt_summary["Total_Dollar_Impact_vs_85EM"] < 0]
    .head()
    .to_string(index=False)
)