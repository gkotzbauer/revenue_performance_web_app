# build_diagnostics_base.py
# Creates the "With Diagnostics Base" outputs from the aggregated weekly file.

import os
import pandas as pd
import numpy as np

# =====================================
# Config
# =====================================
# Preferred input (produced by generate_weekly_outputs.py)
AGG_IN_CSV  = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_agg.csv"
# Generic XLSX fallback (if you export the agg as Excel)
AGG_IN_XLSX = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_agg.xlsx"

OUT_CSV  = "/mnt/data/v2_Rev_Perf_Weekly_Model_With_Diagnostics_Base.csv"
OUT_XLSX = "/mnt/data/v2_Rev_Perf_Weekly_Model_With_Diagnostics_Base.xlsx"

# Performance band thresholds (env-overridable)
THRESH_OVER  = float(os.getenv("PERF_OVER_PCT",  "0.05"))   # +5%
THRESH_UNDER = float(os.getenv("PERF_UNDER_PCT", "-0.05"))  # -5%

# =====================================
# Helpers
# =====================================
def to_float_safe(x):
    """Robust numeric parse: handles %, commas, blanks."""
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

def classify_label(actual, expected):
    """Return performance label using over/under thresholds."""
    if pd.isna(actual) or pd.isna(expected) or expected == 0:
        return "No Data"
    diff_pct = (actual - expected) / expected
    if diff_pct > THRESH_OVER:
        return "Over Performing"
    elif diff_pct < THRESH_UNDER:
        return "Under Performing"
    else:
        return "Average Performance"

def format_pct_columns(df_in, cols):
    """String-format percentage columns for final deliverables."""
    df_out = df_in.copy()
    for col in cols:
        if col in df_out.columns:
            df_out[col] = (df_out[col].astype(float) * 100).round().astype("Int64").astype(str) + "%"
    return df_out

# =====================================
# Step 0: Load aggregated file (CSV preferred, XLSX fallback)
# =====================================
if os.path.isfile(AGG_IN_CSV):
    df = pd.read_csv(AGG_IN_CSV)
elif os.path.isfile(AGG_IN_XLSX):
    df = pd.read_excel(AGG_IN_XLSX)
else:
    # If you sometimes use numbered variants like "..._agg 5.xlsx", add another fallback:
    # detect the first matching XLSX in /mnt/data
    candidates = [p for p in os.listdir("/mnt/data") if p.startswith("v2_Rev_Perf_Weekly_Model_Output_Final_agg") and p.endswith(".xlsx")]
    if candidates:
        df = pd.read_excel(os.path.join("/mnt/data", candidates[0]))
    else:
        raise FileNotFoundError(f"Missing required input file: {AGG_IN_CSV} or {AGG_IN_XLSX}")

# =====================================
# Step 1: Normalize numeric fields
# =====================================
numeric_like_cols = [
    "Payment_Amount",
    "Expected_Payment",
    "benchmark_payment",
    "Visit_Count",
    "Zero_Balance_Collection_Rate",
    "Collection_Rate",
    "Denial_Percent",
    "NRV_Gap_Percent",
    "Remaining_Charges_Percent",
    # Optional extras if present
    "Charge_Billed_Balance",
    "Zero_Balance_Collection_Star_Charges",
    "NRV_Zero_Balance",
    "NRV_Gap_Dollar",
    "NRV_Gap_Sum_Dollar",
    "Expected_Amount_85_EM_invoice_level",
]
for col in numeric_like_cols:
    if col in df.columns:
        df[col] = df[col].apply(to_float_safe)

# Fallback: recompute Expected_Payment if missing from expected rate × visits
if "Expected_Payment" not in df.columns or df["Expected_Payment"].isna().all():
    if "Expected_Amount_85_EM_invoice_level" in df.columns and "Visit_Count" in df.columns:
        df["Expected_Payment"] = df["Expected_Amount_85_EM_invoice_level"] * df["Visit_Count"]
    else:
        raise ValueError("Expected_Payment not present and cannot be derived (missing Expected_Amount_85_EM_invoice_level or Visit_Count).")

# =====================================
# Step 2: Variances & Labels (two lenses)
# =====================================
# --- vs 85% E/M (Expected_Payment)
df["Revenue_Variance_vs_85EM_$"] = df["Payment_Amount"] - df["Expected_Payment"]
df["Revenue_Variance_vs_85EM_%"] = np.where(
    df["Expected_Payment"] == 0, np.nan, df["Revenue_Variance_vs_85EM_$"] / df["Expected_Payment"]
)
df["Performance_Label_vs_85EM"] = df.apply(
    lambda r: classify_label(r["Payment_Amount"], r["Expected_Payment"]), axis=1
)

# --- vs Benchmark (benchmark_payment)
if "benchmark_payment" in df.columns and not df["benchmark_payment"].isna().all():
    df["Revenue_Variance_vs_Benchmark_$"] = df["Payment_Amount"] - df["benchmark_payment"]
    df["Revenue_Variance_vs_Benchmark_%"] = np.where(
        df["benchmark_payment"] == 0, np.nan, df["Revenue_Variance_vs_Benchmark_$"] / df["benchmark_payment"]
    )
    df["Performance_Label_vs_Benchmark"] = df.apply(
        lambda r: classify_label(r["Payment_Amount"], r["benchmark_payment"]), axis=1
    )
else:
    # Keep schema consistent
    df["Revenue_Variance_vs_Benchmark_$"] = np.nan
    df["Revenue_Variance_vs_Benchmark_%"] = np.nan
    df["Performance_Label_vs_Benchmark"] = "No Data"

# =====================================
# Step 3: Baseline averages per Payer + Group_EM + Group_EM2
# =====================================
# Map of metrics we carry baselines for (use current aggregated names)
metric_map = [
    ("Charge_Billed_Balance", "Charge_Billed_Balance"),
    ("Zero_Balance_Collection_Star_Charges", "Zero_Balance_Collection_Star_Charges"),
    ("NRV_Zero_Balance", "NRV_Zero_Balance"),
    ("Zero_Balance_Collection_Rate", "Zero_Balance_Collection_Rate"),
    ("Collection_Rate", "Collection_Rate"),
    ("Payment_Amount", "Payment_Amount"),
    ("Denial_Percent", "Denial_Percent"),
    ("NRV_Gap_Dollar", "NRV_Gap_Dollar"),
    ("NRV_Gap_Percent", "NRV_Gap_Percent"),
    ("Remaining_Charges_Percent", "Remaining_Charges_Percent"),
    ("NRV_Gap_Sum_Dollar", "NRV_Gap_Sum_Dollar"),
]
present_metrics = [m for m in metric_map if m[0] in df.columns]

if present_metrics:
    grp_cols = ["Payer", "Group_EM", "Group_EM2"]
    for g in grp_cols:
        if g not in df.columns:
            raise ValueError(f"Missing grouping column: {g}")

    # Ensure numerics before averaging
    for col, _alias in present_metrics:
        df[col] = df[col].apply(to_float_safe)

    baseline_avgs = (
        df.groupby(grp_cols, dropna=False)[[m[0] for m in present_metrics]]
          .mean(numeric_only=True)
          .rename(columns={m[0]: f"{m[1]}_Avg" for m in present_metrics})
          .reset_index()
    )
    df = df.merge(baseline_avgs, on=grp_cols, how="left")

# =====================================
# Step 4: Optional presentation — string-format percentage columns
# =====================================
pct_cols_to_format = [
    "Zero_Balance_Collection_Rate",
    "Collection_Rate",
    "Denial_Percent",
    "NRV_Gap_Percent",
    "Remaining_Charges_Percent",
    "Revenue_Variance_vs_85EM_%",
    "Revenue_Variance_vs_Benchmark_%",
]
df_out = format_pct_columns(df, pct_cols_to_format)

# =====================================
# Step 5: Export
# =====================================
df_out.to_csv(OUT_CSV, index=False)
try:
    df_out.to_excel(OUT_XLSX, index=False)
except Exception as e:
    # If openpyxl isn't available, at least ship the CSV
    print(f"Note: Excel export skipped ({e}); CSV written to {OUT_CSV}")

print(f"✅ Diagnostics Base exported:\n  CSV  -> {OUT_CSV}\n  XLSX -> {OUT_XLSX}")
