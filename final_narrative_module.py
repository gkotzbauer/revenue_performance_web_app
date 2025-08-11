#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import pandas as pd
import numpy as np

# ======================================
# Inputs (auto-detect best available)
# ======================================
DATA_DIR = Path("/mnt/data")

# Priority order:
#   1) *_ml_boosted.csv  (new step)
#   2) *_agg_ml.csv      (older ML step)
#   3) v2_Rev_Perf_Weekly_Model_With_Diagnostics_Base.(csv|xlsx)
CANDIDATES = [
    "*_ml_boosted.csv",
    "*_agg_ml.csv",
    "v2_Rev_Perf_Weekly_Model_With_Diagnostics_Base.csv",
    "v2_Rev_Perf_Weekly_Model_With_Diagnostics_Base.xlsx",
]

OUT_XLSX = DATA_DIR / "Weekly_Performance_With_Diagnostics.xlsx"

THRESH_OVER = float(os.getenv("PERF_OVER_PCT", "0.05"))
THRESH_UNDER = float(os.getenv("PERF_UNDER_PCT", "-0.05"))

def to_float_safe(x):
    if pd.isna(x): return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)): return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"):
        try: return float(s[:-1]) / 100.0
        except: return np.nan
    try: return float(s)
    except: return np.nan

def load_best_input() -> pd.DataFrame:
    files = []
    for pat in CANDIDATES:
        files += list(DATA_DIR.glob(pat))
    if not files:
        raise FileNotFoundError("No narrative input found in /mnt/data.")
    # Most recent by mtime
    p = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
    else:
        df = pd.read_excel(p)
    print(f"📂 final_narrative_module.py using: {p}")
    return df

def classify_label(actual, expected):
    if pd.isna(actual) or pd.isna(expected) or expected == 0:
        return "No Data"
    diff_pct = (actual - expected) / expected
    if diff_pct > THRESH_OVER:  return "Over Performing"
    if diff_pct < THRESH_UNDER: return "Under Performing"
    return "Average Performance"

# ======================================
# Load
# ======================================
weekly = load_best_input()

# Required groupers
for c in ["Year","Week","Payer","Group_EM","Group_EM2"]:
    if c not in weekly.columns:
        raise ValueError(f"Missing required column: {c}")

# Normalize percent-like columns
percent_like_cols = [
    "Zero_Balance_Collection_Rate",
    "Collection_Rate",
    "Denial_Percent",
    "NRV_Gap_Percent",
    "Remaining_Charges_Percent",
    "Revenue_Variance_vs_85EM_%",
    "Revenue_Variance_vs_Benchmark_%",
]
for c in percent_like_cols:
    if c in weekly.columns:
        weekly[c] = weekly[c].apply(to_float_safe)

# Normalize numeric
for c in [
    "Payment_Amount","Expected_Payment","benchmark_payment",
    "Revenue_Variance_vs_85EM_$","Revenue_Variance_vs_Benchmark_$",
    "Visit_Count","Charge_Billed_Balance","Zero_Balance_Collection_Star_Charges",
    "NRV_Zero_Balance","NRV_Gap_Dollar","NRV_Gap_Sum_Dollar"
]:
    if c in weekly.columns:
        weekly[c] = weekly[c].apply(to_float_safe)

# =========================================================
# 1) Performance labels under both lenses (if not present)
# =========================================================
if "Performance_Label_vs_85EM" not in weekly.columns:
    if {"Payment_Amount","Expected_Payment"}.issubset(weekly.columns):
        weekly["Performance_Label_vs_85EM"] = weekly.apply(
            lambda r: classify_label(r["Payment_Amount"], r["Expected_Payment"]), axis=1
        )
    else:
        weekly["Performance_Label_vs_85EM"] = "No Data"

if "Performance_Label_vs_Benchmark" not in weekly.columns:
    if {"Payment_Amount","benchmark_payment"}.issubset(weekly.columns):
        weekly["Performance_Label_vs_Benchmark"] = weekly.apply(
            lambda r: classify_label(r["Payment_Amount"], r["benchmark_payment"]), axis=1
        )
    else:
        weekly["Performance_Label_vs_Benchmark"] = "No Data"

# =========================================================
# 2) ML awareness: prefer HGB gaps if present
#    Columns created by build_ml_rate_diagnostics_boosted.py:
#      HGB_Expected_Rate_per_Visit, HGB_Rate_Gap, HGB_Dollar_Gap, HGB_Material_Gap_Flag
#    We’ll add two narrative columns that use these signals.
# =========================================================
ml_cols = {"HGB_Expected_Rate_per_Visit","HGB_Rate_Gap","HGB_Dollar_Gap","HGB_Material_Gap_Flag"}
has_hgb = ml_cols.issubset(set(weekly.columns))

if has_hgb:
    # Aggregate ML gaps by grouping to surface hotspots
    grp_cols = ["Year","Week","Payer","Group_EM","Group_EM2"]
    ml_hotspots = (
        weekly.groupby(grp_cols, dropna=False)
              .agg(
                  ML_Dollar_Gap_Sum=("HGB_Dollar_Gap","sum"),
                  ML_Material_Flag_Sum=("HGB_Material_Gap_Flag","sum"),
                  ML_Rate_Gap_Mean=("HGB_Rate_Gap","mean"),
              )
              .reset_index()
    )
    weekly = weekly.merge(ml_hotspots, on=grp_cols, how="left")

    # Narrative snippets
    def ml_snip(row):
        if pd.isna(row.get("ML_Dollar_Gap_Sum", np.nan)):
            return ""
        dollars = row["ML_Dollar_Gap_Sum"]
        flags   = int(row["ML_Material_Flag_Sum"] or 0)
        rategap = row["ML_Rate_Gap_Mean"]
        direction = "under" if dollars < 0 else "over"
        return (f"ML signals {direction}-payment ≈ ${abs(dollars):,.0f} "
                f"({flags} material flags; avg rate gap {rategap:+.2f}/visit).")
else:
    def ml_snip(_): return ""

# =========================================================
# 3) Revenue Cycle narrative diagnostics (updated names)
# =========================================================
metric_map = {
    "Charge Billed Balance": "Charge_Billed_Balance",
    "Zero Balance - Collection * Charges": "Zero_Balance_Collection_Star_Charges",
    "NRV Zero Balance*": "NRV_Zero_Balance",
    "Zero Balance Collection Rate": "Zero_Balance_Collection_Rate",
    "Collection Rate*": "Collection_Rate",
    "Payment Amount*": "Payment_Amount",
    "Denial %": "Denial_Percent",
    "NRV Gap ($)": "NRV_Gap_Dollar",
    "NRV Gap (%)": "NRV_Gap_Percent",
    "% of Remaining Charges": "Remaining_Charges_Percent",
    "NRV Gap Sum ($)": "NRV_Gap_Sum_Dollar",
}
increase_good = {
    "Charge_Billed_Balance": False,
    "Zero_Balance_Collection_Star_Charges": False,
    "NRV_Zero_Balance": True,
    "Zero_Balance_Collection_Rate": True,
    "Collection_Rate": True,
    "Payment_Amount": True,
    "Denial_Percent": False,
    "NRV_Gap_Dollar": False,
    "NRV_Gap_Percent": False,
    "Remaining_Charges_Percent": False,
    "NRV_Gap_Sum_Dollar": False,
}

def prioritized_top6(lst):
    priority_payers = [
        "BCBS","AETNA","MEDICAID","SELF PAY","UNITED HEALTHCARE",
        "CIGNA","HUMANA","TRICARE","MEDICARE"
    ]
    seen = {}
    for pct, txt in lst:
        key = txt.split("from avg")[0].strip()
        payer_prefix = key.split("–")[0].strip().upper()
        prio = priority_payers.index(payer_prefix) if payer_prefix in priority_payers else len(priority_payers)
        if key not in seen or (prio, -pct) < seen[key][0]:
            seen[key] = ((prio, -pct), txt)
    return [v[1] for v in sorted(seen.values(), key=lambda x: x[0])[:6]]

rc_records = []
for (yr, wk), sub in weekly.groupby(["Year","Week"], dropna=False):
    good, bad = [], []
    for _, r in sub.iterrows():
        for legacy, col in metric_map.items():
            avg_col = f"{col}_Avg"
            if col not in r or avg_col not in r:
                continue
            act = to_float_safe(r[col])
            avg = to_float_safe(r[avg_col])
            if pd.isna(act) or pd.isna(avg) or avg == 0: 
                continue
            delta = act - avg
            pct = abs(delta / avg) * 100.0
            txt = f"{r['Payer']} – {r['Group_EM']} {legacy} {'increased' if delta>0 else 'decreased'} from avg {avg:.2f} to {act:.2f}"
            if col == "Zero_Balance_Collection_Star_Charges" and avg < 0:
                if act == 0:
                    good.append((pct, txt))
                elif act > 0:
                    bad.append((pct, txt))
                else:
                    inc_ok = (delta>0 and increase_good[col]) or (delta<0 and not increase_good[col])
                    (good if inc_ok else bad).append((pct, txt))
            else:
                inc_ok = (delta>0 and increase_good[col]) or (delta<0 and not increase_good[col])
                (good if inc_ok else bad).append((pct, txt))
    rc_records.append({
        "Year": yr, "Week": wk,
        "Revenue Cycle - What Went Well": "; ".join(prioritized_top6(good)),
        "Revenue Cycle - What Can Be Improved": "; ".join(prioritized_top6(bad))
    })
rc_df = pd.DataFrame(rc_records)
weekly = weekly.merge(rc_df, on=["Year","Week"], how="left")

# =========================================================
# 4) Boolean flags (both lenses) + ML summaries inline
# =========================================================
def flag_from_label(series, target):
    return (series == target).astype(int)

weekly["Over Performed (85% E/M)"]   = flag_from_label(weekly["Performance_Label_vs_85EM"], "Over Performing")
weekly["Under Performed (85% E/M)"]  = flag_from_label(weekly["Performance_Label_vs_85EM"], "Under Performing")
weekly["Average Performance (85% E/M)"] = flag_from_label(weekly["Performance_Label_vs_85EM"], "Average Performance")

weekly["Over Performed (Benchmark)"]   = flag_from_label(weekly["Performance_Label_vs_Benchmark"], "Over Performing")
weekly["Under Performed (Benchmark)"]  = flag_from_label(weekly["Performance_Label_vs_Benchmark"], "Under Performing")
weekly["Average Performance (Benchmark)"] = flag_from_label(weekly["Performance_Label_vs_Benchmark"], "Average Performance")

weekly["Volume Without Revenue Lift"] = (
    (weekly["Visit_Count"] > weekly["Visit_Count"].mean()) &
    (weekly["Over Performed (85% E/M)"] == 0)
).astype(int)

# Add ML narrative fields (if available)
if has_hgb:
    weekly["ML_Narrative_Summary"] = weekly.apply(ml_snip, axis=1)
else:
    weekly["ML_Narrative_Summary"] = ""

# =========================================================
# 5) Zero-Balance collection narrative (updated)
# =========================================================
for c in ["Zero_Balance_Collection_Rate","Collection_Rate"]:
    if c in weekly.columns:
        weekly[c] = weekly[c].apply(to_float_safe)

group_cols = ["Year","Week","Payer","Group_EM","Group_EM2"]
zb_grp = weekly.groupby(group_cols, dropna=False).agg({
    "Zero_Balance_Collection_Rate": "mean",
    "Collection_Rate": "mean"
}).reset_index()

zb_base = zb_grp.groupby(["Payer","Group_EM","Group_EM2"], dropna=False).agg({
    "Zero_Balance_Collection_Rate": "mean",
    "Collection_Rate": "mean"
}).rename(columns={
    "Zero_Balance_Collection_Rate": "ZBCR_Baseline",
    "Collection_Rate": "CR_Baseline"
}).reset_index()

zb_grp = zb_grp.merge(zb_base, on=["Payer","Group_EM","Group_EM2"], how="left")

def zb_narr(row):
    zb, cr, zb_bl, cr_bl = row["Zero_Balance_Collection_Rate"], row["Collection_Rate"], row["ZBCR_Baseline"], row["CR_Baseline"]
    if pd.isna(zb) or pd.isna(zb_bl) or pd.isna(cr) or pd.isna(cr_bl):
        return "Collection data incomplete"
    if zb < 0.75 * zb_bl:                      return "Below baseline"
    if zb > 1.25 * zb_bl or zb > 1.2 * cr_bl:  return "Above baseline"
    return "Normal range"

zb_grp["Zero-Balance Narrative Text"] = zb_grp.apply(zb_narr, axis=1)
zb_grp["Zero-Balance Collection Narrative"] = (
    zb_grp["Payer"] + " – " +
    zb_grp["Group_EM"] + " – " +
    zb_grp["Group_EM2"] + " – " +
    zb_grp["Zero-Balance Narrative Text"]
)

narr_summary = (
    zb_grp.groupby(["Year","Week"], dropna=False)["Zero-Balance Collection Narrative"]
          .apply(lambda x: "; ".join(sorted(set(x))))
          .reset_index()
)
weekly = weekly.merge(narr_summary, on=["Year","Week"], how="left")

# =========================================================
# 6) Export (Excel deliverable; keep numerics numeric)
# =========================================================
weekly.to_excel(OUT_XLSX, index=False)
print(f"✅ Weekly output with ML-aware narratives written to: {OUT_XLSX}")
