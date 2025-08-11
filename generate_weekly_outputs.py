import os
import pandas as pd
import numpy as np
import zipfile
import ast

# =============================
# Config / Inputs & Outputs
# =============================
PREFERRED_CSV = "invoice_level_index.csv"  # preferred if present
ATTACHED_XLSX_CANDIDATES = [
    "/mnt/data/invoice_level_index_enhanced 4.xlsx",
    "/mnt/data/RMT Invoice_level_index.xlsx",
]

GRANULAR_CSV = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_granular.csv"
GRANULAR_ZIP = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_granular.zip"

# =============================
# Step 1: Load invoice-level data
# =============================
if os.path.isfile(PREFERRED_CSV):
    base_df = pd.read_csv(PREFERRED_CSV)
else:
    path = next((p for p in ATTACHED_XLSX_CANDIDATES if os.path.isfile(p)), None)
    if path is None:
        raise FileNotFoundError(f"Missing '{PREFERRED_CSV}' and none of {ATTACHED_XLSX_CANDIDATES} exists.")
    base_df = pd.read_excel(path)

# Ensure 'Lab per Visit'
if "Lab per Visit" not in base_df.columns and "Lab per Visit (copy)" in base_df.columns:
    base_df["Lab per Visit"] = base_df["Lab per Visit (copy)"]

# Ensure some expected columns exist
for missing_col in ["NRV Gap ($)", "NRV Gap (%)", "NRV Gap Sum ($)"]:
    if missing_col not in base_df.columns:
        base_df[missing_col] = 0.0

# =============================
# Step 2: Numeric coercion
# =============================
num_cols = [
    "Charge Amount", "Payment Amount*", "Avg. Charge E/M Weight", "Lab per Visit",
    "Procedure per Visit", "Radiology Count", "Zero Balance Collection Rate",
    "Collection Rate*", "Denial %", "Charge Billed Balance",
    "Zero Balance - Collection * Charges", "NRV Zero Balance*",
    "NRV Gap ($)", "NRV Gap (%)", "% of Remaining Charges", "NRV Gap Sum ($)",
    "Open Invoice Count", "Expected Amount (85% E/M)"
]
for c in num_cols:
    if c in base_df.columns:
        base_df[c] = pd.to_numeric(base_df[c], errors="coerce")

# =============================
# Step 3: Per-key historical benchmarks
# =============================
# 3A) Expected 85%E/M rate per visit (per Benchmark_Key)
exp_rate_by_key = (
    base_df.groupby("Benchmark_Key", dropna=False)["Expected Amount (85% E/M)"]
           .mean()
           .rename("Expected_Amount_85_EM_invoice_level")
           .reset_index()
)

# 3B) Historical mean weekly visits (for context; used for Volume_Gap vs visits)
weekly_visits_by_key = (
    base_df.groupby(["Benchmark_Key","Year","Week"], dropna=False)["Invoice_Number"]
           .nunique()
           .rename("Visit_Count_Weekly")
           .reset_index()
)
bench_inv_count = (
    weekly_visits_by_key.groupby("Benchmark_Key", dropna=False)["Visit_Count_Weekly"]
                        .mean()
                        .rename("Benchmark_Invoice_Count")
                        .reset_index()
)

# 3C) Historical payment rate per visit (per Benchmark_Key)
weekly_key_totals = (
    base_df.groupby(["Benchmark_Key","Year","Week"], dropna=False)
           .agg(Payment_Amount_week=("Payment Amount*", "sum"),
                Visit_Count_week=("Invoice_Number", "nunique"))
           .reset_index()
)
weekly_key_totals["Benchmark_Payment_Rate_week"] = np.where(
    weekly_key_totals["Visit_Count_week"] == 0,
    np.nan,
    weekly_key_totals["Payment_Amount_week"] / weekly_key_totals["Visit_Count_week"]
)
bench_pay_rate_by_key = (
    weekly_key_totals.groupby("Benchmark_Key", dropna=False)["Benchmark_Payment_Rate_week"]
                     .mean(skipna=True)
                     .rename("Benchmark_Payment_Rate_per_Visit")
                     .reset_index()
)

# =============================
# Step 4: Weekly granular aggregation (Benchmark_Key)
# =============================
group_cols_granular = ['Year', 'Week', 'Payer', 'Group_EM', 'Group_EM2', 'Benchmark_Key']

weekly = (
    base_df.groupby(group_cols_granular, dropna=False)
    .agg(
        Visit_Count=('Invoice_Number', 'nunique'),
        Group_Size=('Invoice_Number', 'count'),
        Charge_Amount=('Charge Amount', 'sum'),
        Payment_Amount=('Payment Amount*', 'sum'),
        Avg_Charge_EM_Weight=('Avg. Charge E/M Weight', 'mean'),
        Labs_per_Visit=('Lab per Visit', 'mean'),
        Procedure_per_Visit=('Procedure per Visit', 'mean'),
        Radiology_Count=('Radiology Count', 'mean'),
        Zero_Balance_Collection_Rate=('Zero Balance Collection Rate', 'mean'),
        Collection_Rate=('Collection Rate*', 'mean'),
        Denial_Percent=('Denial %', 'mean'),
        Charge_Billed_Balance=('Charge Billed Balance', 'sum'),
        Zero_Balance_Collection_Star_Charges=('Zero Balance - Collection * Charges', 'sum'),
        NRV_Zero_Balance=('NRV Zero Balance*', 'sum'),
        NRV_Gap_Dollar=('NRV Gap ($)', 'sum'),
        NRV_Gap_Percent=('NRV Gap (%)', 'mean'),
        Remaining_Charges_Percent=('% of Remaining Charges', 'mean'),
        NRV_Gap_Sum_Dollar=('NRV Gap Sum ($)', 'sum'),
        Open_Invoice_Count=('Open Invoice Count', 'sum')
    )
    .reset_index()
)

# Merge per-key benchmarks
weekly = weekly.merge(exp_rate_by_key, on="Benchmark_Key", how="left")
weekly = weekly.merge(bench_inv_count, on="Benchmark_Key", how="left")
weekly = weekly.merge(bench_pay_rate_by_key, on="Benchmark_Key", how="left")

# CPT count from key string
def count_cpts(key):
    try:
        cpts = ast.literal_eval(str(key).split('|')[-1])
        return len(cpts) if isinstance(cpts, list) else 0
    except Exception:
        return 0
weekly['CPT_Count'] = weekly['Benchmark_Key'].apply(count_cpts)

# =============================
# Step 5: Granular derived metrics
# =============================
weekly['Expected_Payment'] = weekly['Expected_Amount_85_EM_invoice_level'] * weekly['Visit_Count']
weekly['Benchmark_Payment'] = weekly['Benchmark_Payment_Rate_per_Visit'] * weekly['Visit_Count']

weekly['Actual_Rate_per_Visit'] = np.where(
    weekly['Visit_Count'] == 0, np.nan, weekly['Payment_Amount'] / weekly['Visit_Count']
)
weekly['Revenue_Variance'] = weekly['Payment_Amount'] - weekly['Expected_Payment']
weekly['Revenue_Variance_Pct'] = np.where(
    weekly['Expected_Payment'] == 0, np.nan, weekly['Revenue_Variance'] / weekly['Expected_Payment']
)
weekly['Volume_Gap'] = weekly['Visit_Count'] - weekly['Benchmark_Invoice_Count']
weekly['Rate_Variance'] = weekly['Actual_Rate_per_Visit'] - weekly['Expected_Amount_85_EM_invoice_level']

# =============================
# Step 6: ADD group-level benchmark diagnostics onto each granular row
# =============================
group_cols_group = ['Year', 'Week', 'Payer', 'Group_EM', 'Group_EM2']
MATERIALITY_PCT = 0.03  # 3% threshold (same as aggregated)

# Compute weighted & unweighted benchmark_payment at the group level from granular rows
group_weighting = (
    weekly.assign(_w=weekly["Benchmark_Payment_Rate_per_Visit"] * weekly["Visit_Count"])
          .groupby(group_cols_group, dropna=False)
          .agg(
              Group_benchmark_payment_weighted=("_w","sum"),
              Group_total_visits=("Visit_Count","sum"),
              Group_mean_rate_unweighted=("Benchmark_Payment_Rate_per_Visit","mean")
          )
          .reset_index()
)
group_weighting["Group_benchmark_payment_unweighted"] = (
    group_weighting["Group_mean_rate_unweighted"] * group_weighting["Group_total_visits"]
)
group_weighting["Group_Benchmark_Payment_Diff_$"] = (
    group_weighting["Group_benchmark_payment_weighted"] - group_weighting["Group_benchmark_payment_unweighted"]
)
group_weighting["Group_Benchmark_Payment_Diff_%"] = np.where(
    group_weighting["Group_benchmark_payment_unweighted"] == 0,
    np.nan,
    group_weighting["Group_Benchmark_Payment_Diff_$"] / group_weighting["Group_benchmark_payment_unweighted"]
)
group_weighting["Group_Benchmark_Payment_Material_Flag"] = (
    group_weighting["Group_Benchmark_Payment_Diff_%"].abs() >= MATERIALITY_PCT
)

# Also attach the group's unique invoice count (ground truth)
group_invoice_counts = (
    base_df.groupby(group_cols_group, dropna=False)["Invoice_Number"]
           .nunique()
           .rename("Group_Benchmark_Invoice_Count")
           .reset_index()
)

# Merge group diagnostics into each granular row
weekly = weekly.merge(group_weighting, on=group_cols_group, how="left")
weekly = weekly.merge(group_invoice_counts, on=group_cols_group, how="left")

# =============================
# Step 7: Percent formatting (end)
# =============================
def format_pct_columns(df_in, cols):
    df_out = df_in.copy()
    for col in cols:
        if col in df_out.columns:
            df_out[col] = (df_out[col].astype(float) * 100).round().astype('Int64').astype(str) + '%'
    return df_out

pct_cols = [
    'Zero_Balance_Collection_Rate', 'Collection_Rate', 'Denial_Percent',
    'NRV_Gap_Percent', 'Remaining_Charges_Percent', 'Revenue_Variance_Pct',
    'Group_Benchmark_Payment_Diff_%'
]
weekly_out = format_pct_columns(weekly, pct_cols)

# =============================
# Step 8: Export
# =============================
weekly_out.to_csv(GRANULAR_CSV, index=False)
with zipfile.ZipFile(GRANULAR_ZIP, "w", zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(GRANULAR_CSV, arcname=os.path.basename(GRANULAR_CSV))

print(f"✅ Granular CPT-level export (with group diagnostics): {GRANULAR_ZIP}")
