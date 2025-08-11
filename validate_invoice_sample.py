import os
import sys
import pandas as pd
import numpy as np

# =========================
# File paths (update if needed)
# =========================
SOURCE_FILE = "/mnt/data/Invoice_Assigned_To_Benchmark_With_Count v3.xlsx"  # raw Excel
PROCESSED_FILE = "/mnt/data/invoice_level_index.csv"                         # enhanced invoice CSV

VALIDATION_OUTPUT = "/mnt/data/benchmark_validation_comparison.csv"
DISCREPANCY_OUTPUT = "/mnt/data/benchmark_discrepancy_report.csv"

# NEW: sample-based outputs
SAMPLE_DETAILS_OUT = "/mnt/data/benchmark_validation_sample_details.csv"
SAMPLE_SUMMARY_OUT = "/mnt/data/benchmark_validation_sample_summary.csv"
SAMPLE_MISMATCH_OUT = "/mnt/data/benchmark_validation_sample_mismatches.csv"

# =========================
# Config (env or CLI)
# =========================
def getenv_default(name, default):
    return os.getenv(name, default)

SAMPLE_SIZE = int(getenv_default("SAMPLE_SIZE", "30"))
SAMPLE_SEED = int(getenv_default("SAMPLE_SEED", "42"))

# Allow CLI overrides: python validate_invoice_sample.py 50 123
if len(sys.argv) >= 2:
    SAMPLE_SIZE = int(sys.argv[1])
if len(sys.argv) >= 3:
    SAMPLE_SEED = int(sys.argv[2])

# =========================
# Helpers
# =========================
def to_float_safe(x):
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

def isclose_series(a, b, rtol=1e-2, atol=1e-6):
    a = a.astype(float)
    b = b.astype(float)
    both_nan = a.isna() & b.isna()
    cmp = pd.Series(np.isclose(a.fillna(0), b.fillna(0), rtol=rtol, atol=atol), index=a.index)
    return both_nan | cmp

def require_cols(df, cols, name="dataframe"):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {name}: {missing}")

# =========================
# Step 0: Guard files
# =========================
if not os.path.exists(SOURCE_FILE):
    raise FileNotFoundError(f"Source file missing: {SOURCE_FILE}")
if not os.path.exists(PROCESSED_FILE):
    raise FileNotFoundError(f"Processed output file missing: {PROCESSED_FILE}")

# =========================
# Step 1: Load
# =========================
source_df = pd.read_excel(SOURCE_FILE)
processed_df = pd.read_csv(PROCESSED_FILE)

# Normalize core ID columns (string, trimmed)
for col in ["Invoice_Number", "Payer", "Group_EM", "Group_EM2", "Charge CPT Code"]:
    if col in source_df.columns:
        source_df[col] = source_df[col].astype(str).str.strip()

# =========================
# Step 2: Build Benchmark_Key from source like preprocess step
# =========================
require_cols(source_df, ["Invoice_Number", "Payer", "Group_EM", "Group_EM2", "Charge CPT Code"], "source_df")

cpt_list_df = (
    source_df.groupby("Invoice_Number", dropna=False)["Charge CPT Code"]
    .apply(lambda x: sorted(set(map(lambda z: str(z).strip(), x))))
    .reset_index()
    .rename(columns={"Charge CPT Code": "CPT_List"})
)
cpt_list_df["CPT_List_Str"] = cpt_list_df["CPT_List"].apply(str)
source_df = source_df.merge(cpt_list_df, on="Invoice_Number", how="left")

source_df["Benchmark_Key"] = (
    source_df["Payer"].astype(str) + "|" +
    source_df["Group_EM"].astype(str) + "|" +
    source_df["Group_EM2"].astype(str) + "|" +
    source_df["CPT_List_Str"].astype(str)
)

# =========================
# Step 3: Numeric coercion in source
# =========================
num_cols_source = [
    "Payment Amount*", "Expected Amount (85% E/M)", "Charge Billed Balance",
    "NRV Gap ($)", "NRV Gap (%)", "NRV Gap Sum ($)"
]
for c in num_cols_source:
    if c in source_df.columns:
        source_df[c] = source_df[c].apply(to_float_safe)

# =========================
# Step 4: Recompute key-level benchmarks from source
# =========================
# A) Expected rate per key
expected_rate_by_key = (
    source_df.groupby("Benchmark_Key", dropna=False)["Expected Amount (85% E/M)"]
             .mean()
             .rename("Expected_Amount_85_EM_invoice_level_recalc")
             .reset_index()
)

# B) Simple row-level mean payment per key
benchmark_payment_mean_row = (
    source_df.groupby("Benchmark_Key", dropna=False)["Payment Amount*"]
             .mean()
             .rename("Benchmark_Payment_Amount_invoice_level_recalc")
             .reset_index()
)

# C) Per-invoice average payment per key:
invoice_totals = (
    source_df.groupby(["Benchmark_Key", "Invoice_Number"], dropna=False)["Payment Amount*"]
             .sum()
             .rename("Invoice_Total_Payment")
             .reset_index()
)
benchmark_payment_mean_invoice = (
    invoice_totals.groupby("Benchmark_Key", dropna=False)["Invoice_Total_Payment"]
                  .mean()
                  .rename("Benchmark_Avg_Payment_InvoiceLevel_recalc")
                  .reset_index()
)

recalc = expected_rate_by_key.merge(benchmark_payment_mean_row, on="Benchmark_Key", how="outer") \
                             .merge(benchmark_payment_mean_invoice, on="Benchmark_Key", how="outer")

# =========================
# Step 5: Row-level passthrough checks (from source -> processed)
# =========================
require_cols(processed_df, ["Benchmark_Key", "Invoice_Number"], "processed_df")

row_passthrough_cols = {
    "NRV Gap ($)": "NRV_Gap_Dollar_invoice_level",
    "NRV Gap (%)": "NRV_Gap_Percent_invoice_level",
    "NRV Gap Sum ($)": "NRV_Gap_Sum_Dollar_invoice_level"
}
row_src = source_df[["Benchmark_Key", "Invoice_Number", *row_passthrough_cols.keys()]].copy()
for c in row_passthrough_cols.keys():
    row_src[c] = row_src[c].apply(to_float_safe)

present_proc_cols = [v for v in row_passthrough_cols.values() if v in processed_df.columns]
row_proc = processed_df[["Benchmark_Key", "Invoice_Number", *present_proc_cols]].copy()
for c in present_proc_cols:
    row_proc[c] = row_proc[c].apply(to_float_safe)

row_merged = row_proc.merge(row_src, on=["Benchmark_Key", "Invoice_Number"], how="left", suffixes=("", "_src"))

for src_col, proc_col in row_passthrough_cols.items():
    if proc_col in row_merged.columns:
        flag_col = f"{proc_col}_RowMatch"
        row_merged[flag_col] = isclose_series(row_merged[proc_col], row_merged.get(src_col, np.nan), rtol=1e-6, atol=1e-6)

# =========================
# Step 6: Merge recomputed key-level metrics to processed and compare
# =========================
key_merged = processed_df.merge(recalc, on="Benchmark_Key", how="left", suffixes=("", "_recalc"))

targets = [
    "Expected_Amount_85_EM_invoice_level",
    "Benchmark_Payment_Amount_invoice_level",        # old-style; may be absent
    "Benchmark_Avg_Payment_InvoiceLevel",            # if persisted (_temp or final)
    "Invoice_Total_Payment_temp",                    # temp columns if present
    "Benchmark_Avg_Payment_temp"
]
for t in targets:
    if t in key_merged.columns:
        key_merged[t] = key_merged[t].apply(to_float_safe)

compare_pairs = []
if "Expected_Amount_85_EM_invoice_level" in key_merged.columns:
    compare_pairs.append(("Expected_Amount_85_EM_invoice_level", "Expected_Amount_85_EM_invoice_level_recalc"))
if "Benchmark_Payment_Amount_invoice_level" in key_merged.columns:
    compare_pairs.append(("Benchmark_Payment_Amount_invoice_level", "Benchmark_Payment_Amount_invoice_level_recalc"))
if "Benchmark_Avg_Payment_InvoiceLevel" in key_merged.columns:
    compare_pairs.append(("Benchmark_Avg_Payment_InvoiceLevel", "Benchmark_Avg_Payment_InvoiceLevel_recalc"))
if "Invoice_Total_Payment_temp" in key_merged.columns:
    # Compare invoice-level total against per-invoice recompute by joining invoice_totals
    key_merged = key_merged.merge(invoice_totals, on=["Benchmark_Key", "Invoice_Number"], how="left")
    compare_pairs.append(("Invoice_Total_Payment_temp", "Invoice_Total_Payment"))
if "Benchmark_Avg_Payment_temp" in key_merged.columns and "Benchmark_Avg_Payment_InvoiceLevel_recalc" in key_merged.columns:
    compare_pairs.append(("Benchmark_Avg_Payment_temp", "Benchmark_Avg_Payment_InvoiceLevel_recalc"))

for proc_col, recalc_col in compare_pairs:
    if recalc_col not in key_merged.columns:
        continue
    dcol = f"{proc_col}__Delta"
    mcol = f"{proc_col}__Match"
    key_merged[dcol] = key_merged[proc_col].astype(float) - key_merged[recalc_col].astype(float)
    key_merged[mcol] = isclose_series(key_merged[proc_col], key_merged[recalc_col], rtol=1e-3, atol=1e-6)

# =========================
# Step 7: Write baseline outputs for recompute validation
# =========================
export = key_merged.merge(
    row_merged[["Benchmark_Key", "Invoice_Number"] + [c for c in row_merged.columns if c.endswith("_RowMatch")]],
    on=["Benchmark_Key", "Invoice_Number"],
    how="left"
)
export.to_csv(VALIDATION_OUTPUT, index=False)

flag_cols = [c for c in export.columns if c.endswith("__Match")] + [c for c in export.columns if c.endswith("_RowMatch")]
discrepancies = export.loc[~export[flag_cols].all(axis=1)].copy()
discrepancies.to_csv(DISCREPANCY_OUTPUT, index=False)

# =========================
# Step 8: Sample-based validator (NEW)
# Randomly select N invoices and recompute end-to-end comparisons.
# =========================
np.random.seed(SAMPLE_SEED)

# Sample distinct invoices from the source (ensures we validate what's feeding the pipeline)
unique_invoices = source_df["Invoice_Number"].dropna().unique()
n = min(SAMPLE_SIZE, len(unique_invoices))
sampled_invoices = np.random.choice(unique_invoices, size=n, replace=False)

# Build sample frames
src_samp = source_df[source_df["Invoice_Number"].isin(sampled_invoices)].copy()
proc_samp = processed_df[processed_df["Invoice_Number"].astype(str).isin(sampled_invoices.astype(str))].copy()

# Coerce core numerics in sampled sets
for c in ["Payment Amount*", "Expected Amount (85% E/M)", "Charge Billed Balance"]:
    if c in src_samp.columns:
        src_samp[c] = src_samp[c].apply(to_float_safe)

for c in [
    "Payment Amount*", "Expected Amount (85% E/M)", "Charge Amount", "Payment Amount*",
    "Expected_Amount_85_EM_invoice_level", "Invoice_Total_Payment_temp",
    "Benchmark_Avg_Payment_InvoiceLevel", "Benchmark_Avg_Payment_temp"
]:
    if c in proc_samp.columns:
        proc_samp[c] = proc_samp[c].apply(to_float_safe)

# Recompute invoice-level totals & expected values from source
samp_invoice_totals = (
    src_samp.groupby(["Benchmark_Key", "Invoice_Number"], dropna=False)["Payment Amount*"]
            .sum()
            .rename("Invoice_Total_Payment_recalc")
            .reset_index()
)

# Recompute key-level expected rate (from source) for the keys present in the sample
samp_expected_rate = (
    src_samp.groupby("Benchmark_Key", dropna=False)["Expected Amount (85% E/M)"]
            .mean()
            .rename("Expected_Amount_85_EM_invoice_level_recalc")
            .reset_index()
)

# Attach recomputes to processed sample
sample_compare = proc_samp.merge(samp_invoice_totals, on=["Benchmark_Key", "Invoice_Number"], how="left")
sample_compare = sample_compare.merge(samp_expected_rate, on="Benchmark_Key", how="left")

# Build check specs (only add checks for columns present)
checks = []

# 1) Invoice total payment (processed temp vs recompute)
if "Invoice_Total_Payment_temp" in sample_compare.columns:
    checks.append(("Invoice_Total_Payment_temp", "Invoice_Total_Payment_recalc", 1e-3, 1e-6))

# 2) Expected rate per key (processed vs recompute)
if "Expected_Amount_85_EM_invoice_level" in sample_compare.columns and \
   "Expected_Amount_85_EM_invoice_level_recalc" in sample_compare.columns:
    checks.append(("Expected_Amount_85_EM_invoice_level", "Expected_Amount_85_EM_invoice_level_recalc", 1e-3, 1e-6))

# 3) Against-benchmark variance (requires Benchmark_Avg_Payment_InvoiceLevel or _temp)
#    Recompute per-invoice variance: total_payment - benchmark_avg (key-level)
if "Benchmark_Avg_Payment_InvoiceLevel" in sample_compare.columns or "Benchmark_Avg_Payment_temp" in sample_compare.columns:
    # Need a key-level benchmark average payment (from entire raw source, not just sample)
    # Use earlier 'benchmark_payment_mean_invoice' from full source_df:
    key_bench_inv = benchmark_payment_mean_invoice.rename(columns={
        "Benchmark_Avg_Payment_InvoiceLevel_recalc": "Key_Benchmark_Avg_Payment_InvoiceLevel"
    })
    sample_compare = sample_compare.merge(key_bench_inv, on="Benchmark_Key", how="left")
    sample_compare["Variance_$_Against_Benchmark_recalc"] = (
        sample_compare["Invoice_Total_Payment_recalc"] - sample_compare["Key_Benchmark_Avg_Payment_InvoiceLevel"]
    )

    # If processed has per-invoice variance temp, compare that too (optional)
    if "Revenue_Variance_$_Against_Benchmark" in sample_compare.columns:
        checks.append(("Revenue_Variance_$_Against_Benchmark", "Variance_$_Against_Benchmark_recalc", 1e-2, 1e-6))

# Execute checks and collect results
records = []
for (proc_col, recalc_col, rtol, atol) in checks:
    if recalc_col not in sample_compare.columns:
        continue
    diff = sample_compare[proc_col].astype(float) - sample_compare[recalc_col].astype(float)
    match = np.isclose(sample_compare[proc_col].fillna(0).astype(float),
                       sample_compare[recalc_col].fillna(0).astype(float),
                       rtol=rtol, atol=atol)
    rec = sample_compare[["Benchmark_Key", "Invoice_Number"]].copy()
    rec["Check"] = f"{proc_col} vs {recalc_col}"
    rec["Processed_Value"] = sample_compare[proc_col]
    rec["Recalc_Value"] = sample_compare[recalc_col]
    rec["Delta"] = diff
    rec["Match"] = match
    records.append(rec)

if records:
    sample_details = pd.concat(records, ignore_index=True)
else:
    sample_details = pd.DataFrame(columns=["Benchmark_Key","Invoice_Number","Check","Processed_Value","Recalc_Value","Delta","Match"])

# Save sample details and summary
sample_details.to_csv(SAMPLE_DETAILS_OUT, index=False)

if not sample_details.empty:
    sample_summary = (
        sample_details.groupby("Check")
                      .agg(Total=("Match","size"), Pass=("Match","sum"))
                      .assign(Fail=lambda d: d["Total"] - d["Pass"],
                              Pass_Rate=lambda d: d["Pass"] / d["Total"])
                      .reset_index()
    )
else:
    sample_summary = pd.DataFrame(columns=["Check","Total","Pass","Fail","Pass_Rate"])

sample_summary.to_csv(SAMPLE_SUMMARY_OUT, index=False)

sample_mismatches = sample_details[~sample_details["Match"].astype(bool)].copy()
sample_mismatches.to_csv(SAMPLE_MISMATCH_OUT, index=False)

# =========================
# Final status
# =========================
print("✅ Benchmark validation complete.")
print(f"📄 Full recompute comparison:     {VALIDATION_OUTPUT}")
print(f"⚠️ Discrepancies (recompute):     {DISCREPANCY_OUTPUT}")
print(f"🔍 Sample details (per invoice):  {SAMPLE_DETAILS_OUT}")
print(f"📊 Sample summary:                {SAMPLE_SUMMARY_OUT}")
print(f"❗ Sample mismatches only:        {SAMPLE_MISMATCH_OUT}")
