#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3: Build Underpayment Summary (totals)
-------------------------------------------
Consumes the aggregated weekly model output (from Step 2).
Produces a quick table of total underpayments by payer / week / group.

Key points:
- Reads *_agg.csv (85% E/M + Benchmark metrics)
- Filters for negative revenue variances only (underpayments)
- Sums $ gaps for both 85% E/M Expected Payment and Benchmark Payment
- Outputs: underpayment_summary.csv
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
    "Year", "Week", "Payer", "Group_EM", "Group_EM2",
    "Benchmark_Key",
    "Revenue_Variance_85EM", "Revenue_Variance_Pct_85EM",
    "Revenue_Variance_Benchmark", "Revenue_Variance_Pct_Benchmark"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# ---------------------------
# Filter to underpayments (negative variances)
# ---------------------------
df_under_85 = df[df["Revenue_Variance_85EM"] < 0].copy()
df_under_bench = df[df["Revenue_Variance_Benchmark"] < 0].copy()

# ---------------------------
# Summarize by Week / Payer / Group
# ---------------------------
summary_85 = (
    df_under_85
    .groupby(["Year", "Week", "Payer", "Group_EM", "Group_EM2"], dropna=False)
    .agg(
        Total_Underpayment_85EM=("Revenue_Variance_85EM", "sum"),
        Avg_Underpayment_Pct_85EM=("Revenue_Variance_Pct_85EM", "mean"),
        Records_85EM=("Revenue_Variance_85EM", "count"),
    )
    .reset_index()
)

summary_bench = (
    df_under_bench
    .groupby(["Year", "Week", "Payer", "Group_EM", "Group_EM2"], dropna=False)
    .agg(
        Total_Underpayment_Benchmark=("Revenue_Variance_Benchmark", "sum"),
        Avg_Underpayment_Pct_Benchmark=("Revenue_Variance_Pct_Benchmark", "mean"),
        Records_Benchmark=("Revenue_Variance_Benchmark", "count"),
    )
    .reset_index()
)

# ---------------------------
# Merge the two summaries
# ---------------------------
summary = pd.merge(
    summary_85,
    summary_bench,
    on=["Year", "Week", "Payer", "Group_EM", "Group_EM2"],
    how="outer"
).fillna(0)

# ---------------------------
# Sort and save
# ---------------------------
summary = summary.sort_values(["Year", "Week", "Payer", "Group_EM", "Group_EM2"])
out_path = OUTPUTS_DIR / "underpayment_summary.csv"
summary.to_csv(out_path, index=False)

print(f"✅ Underpayment summary written to {out_path}")
print(f"   Rows: {len(summary)}")

# ---------------------------
# Optional: print top 5 largest gaps for sanity check
# ---------------------------
print("\n🔍 Top 5 Largest Underpayments (85% E/M):")
print(
    summary.sort_values("Total_Underpayment_85EM")
    [["Year", "Week", "Payer", "Group_EM", "Group_EM2", "Total_Underpayment_85EM"]]
    .head()
)

print("\n🔍 Top 5 Largest Underpayments (Benchmark):")
print(
    summary.sort_values("Total_Underpayment_Benchmark")
    [["Year", "Week", "Payer", "Group_EM", "Group_EM2", "Total_Underpayment_Benchmark"]]
    .head()
)