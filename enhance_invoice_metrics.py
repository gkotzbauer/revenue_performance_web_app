import pandas as pd
import numpy as np
import os
import re
import zipfile

# === File Paths ===
INPUT_CSV = "invoice_level_index.csv"
INPUT_XLSX_ENH = "/mnt/data/invoice_level_index_enhanced.xlsx"
INPUT_XLSX_RMT = "/mnt/data/RMT Invoice_level_index.xlsx"
OUTPUT_CSV = "/mnt/data/invoice_level_index_enhanced.csv"
OUTPUT_ZIP = "/mnt/data/invoice_level_index_enhanced.zip"

# === Step 1: Load Data ===
if os.path.isfile(INPUT_CSV):
    df = pd.read_csv(INPUT_CSV)
elif os.path.isfile(INPUT_XLSX_ENH):
    df = pd.read_excel(INPUT_XLSX_ENH)
elif os.path.isfile(INPUT_XLSX_RMT):
    df = pd.read_excel(INPUT_XLSX_RMT)
else:
    raise FileNotFoundError(
        f"Missing file: '{INPUT_CSV}' or '{INPUT_XLSX_ENH}' or '{INPUT_XLSX_RMT}'"
    )

# Normalize Invoice_Number name if needed
alt_invoice_cols = [c for c in df.columns if c.strip().lower() in {
    "invoice_number", "invoice number", "charge invoice number"
}]
if "Invoice_Number" not in df.columns and alt_invoice_cols:
    df.rename(columns={alt_invoice_cols[0]: "Invoice_Number"}, inplace=True)

# === Step 1B: Remove any 'total' summary rows BEFORE processing ===
total_candidate_cols = [
    "Year", "Week", "Payer", "Group_EM", "Group_EM2",
    "Invoice_Number", "Charge CPT Code", "CPT_List_Str"
]
present_total_cols = [c for c in total_candidate_cols if c in df.columns]

def is_total_string(val: object) -> bool:
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return bool(re.fullmatch(r"(grand\s+total|total)", s))

if present_total_cols:
    total_mask = df[present_total_cols].applymap(is_total_string).any(axis=1)
    if "Year" in df.columns and "Week" in df.columns:
        total_mask = total_mask | (
            df["Week"].astype(str).str.strip().str.lower().eq("0") &
            df["Year"].astype(str).str.strip().str.lower().str.contains(r"\bgrand\s*total\b", regex=True)
        )
    rows_before = len(df)
    df = df[~total_mask].copy()
    rows_removed = rows_before - len(df)
else:
    rows_removed = 0

# === Step 2: Ensure Numeric Columns ===
numeric_cols = [
    "Payment Amount*", "Expected Amount (85% E/M)", "Open Invoice Count",
    "Zero Balance Collection Rate", "Collection Rate*",
    "SP Charge Billed Balance", "Insurance Charge Billed Balance",
    "Charge Amount", "Payment per Visit", "Fee Schedule Expected Amount",
    "NRV Gap ($)", "NRV Gap (%)", "NRV Gap Sum ($)",
    "Benchmark_Payment_Amount_invoice_level"
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# === Step 3: Revenue Variance vs 85% E/M (RENAMED ONLY) ===
# We DO NOT create legacy Revenue_Variance_$ or Revenue_Variance_% anymore.
if "Payment Amount*" in df.columns and "Expected Amount (85% E/M)" in df.columns:
    df["Revenue_Variance_$_Against_85%E/M"] = df["Payment Amount*"] - df["Expected Amount (85% E/M)"]
    df["Revenue_Variance_%_Against_85%E/M"] = np.where(
        df["Expected Amount (85% E/M)"] == 0,
        0,  # change to np.nan if preferred
        df["Revenue_Variance_$_Against_85%E/M"] / df["Expected Amount (85% E/M)"]
    )

# === Step 3B: Revenue Variance vs Benchmark AVERAGE (INVOICE-LEVEL) ===
# Build/keep grouping key in temp columns where needed
if "Benchmark_Key" in df.columns:
    group_key_col = "Benchmark_Key"
else:
    if set(["Payer", "Group_EM", "Group_EM2", "CPT_List_Str"]).issubset(df.columns):
        df["Derived_Benchmark_Key_temp"] = (
            df["Payer"].astype(str) + "|" +
            df["Group_EM"].astype(str) + "|" +
            df["Group_EM2"].astype(str) + "|" +
            df["CPT_List_Str"].astype(str)
        )
        group_key_col = "Derived_Benchmark_Key_temp"
    elif set(["Payer", "Group_EM", "Group_EM2"]).issubset(df.columns):
        df["Derived_Benchmark_Key_temp"] = (
            df["Payer"].astype(str) + "|" +
            df["Group_EM"].astype(str) + "|" +
            df["Group_EM2"].astype(str)
        )
        group_key_col = "Derived_Benchmark_Key_temp"
    else:
        raise ValueError("Cannot determine a benchmark key. Provide Benchmark_Key or Payer|Group_EM|Group_EM2[|CPT_List_Str].")

df["Benchmark_Group_Key_temp"] = df[group_key_col]

# Require Invoice_Number to compute invoice-level averages
if "Invoice_Number" not in df.columns:
    raise ValueError("Missing 'Invoice_Number'—required to compute invoice-level averages.")

# 1) Total payment per invoice within a benchmark group
invoice_totals = (
    df.groupby([group_key_col, "Invoice_Number"], dropna=False)["Payment Amount*"]
      .sum()
      .reset_index()
      .rename(columns={"Payment Amount*": "Invoice_Total_Payment_temp"})
)

# 2) Average of invoice totals per benchmark group
bench_avg_invoice = (
    invoice_totals.groupby(group_key_col, dropna=False)["Invoice_Total_Payment_temp"]
      .mean()
      .reset_index()
      .rename(columns={"Invoice_Total_Payment_temp": "Benchmark_Avg_Payment_temp"})
)

# 3) Merge back to row-level DF so each row carries invoice total + group avg
df = df.merge(invoice_totals, on=[group_key_col, "Invoice_Number"], how="left")
df = df.merge(bench_avg_invoice, on=group_key_col, how="left")

# 4) Against-benchmark metrics (per-invoice vs group invoice-average)
df["Revenue_Variance_$_Against_Benchmark"] = (
    df["Invoice_Total_Payment_temp"] - df["Benchmark_Avg_Payment_temp"]
)
df["Revenue_Variance_%_Against_Benchmark"] = np.where(
    df["Benchmark_Avg_Payment_temp"] == 0,
    0,
    df["Revenue_Variance_$_Against_Benchmark"] / df["Benchmark_Avg_Payment_temp"]
)

# === Step 4: Overpayment + Underpayment (based on 85% E/M variance) ===
rev_var_col = (
    "Revenue_Variance_$_Against_85%E/M"
    if "Revenue_Variance_$_Against_85%E/M" in df.columns
    else None
)
if rev_var_col and "Expected Amount (85% E/M)" in df.columns:
    df["Overpayment ($)"] = df[rev_var_col].apply(lambda x: x if pd.notna(x) and x > 0 else 0)
    df["Overpayment (%)"] = np.where(
        df["Expected Amount (85% E/M)"] == 0, 0, df["Overpayment ($)"] / df["Expected Amount (85% E/M)"]
    )
    df["Underpayment ($)"] = df[rev_var_col].apply(lambda x: -x if pd.notna(x) and x < 0 else 0)
    df["Underpayment (%)"] = np.where(
        df["Expected Amount (85% E/M)"] == 0, 0, df["Underpayment ($)"] / df["Expected Amount (85% E/M)"]
    )

# === Step 5: Open Invoice Anomaly ===
if set(["Open Invoice Count", "Zero Balance Collection Rate", "Collection Rate*"]).issubset(df.columns):
    df["Open_Invoice_Anomaly_Flag"] = np.where(
        (df["Open Invoice Count"] == 0) &
        (df["Zero Balance Collection Rate"] < df["Collection Rate*"]),
        True, False
    )

# === Step 6: Positive Balances Only ===
if "SP Charge Billed Balance" in df.columns:
    df["SP_Positive_Balance"] = df["SP Charge Billed Balance"].apply(lambda x: x if pd.notna(x) and x > 0 else 0)
if "Insurance Charge Billed Balance" in df.columns:
    df["Insurance_Positive_Balance"] = df["Insurance Charge Billed Balance"].apply(lambda x: x if pd.notna(x) and x > 0 else 0)

# === Step 7: Invoice-Level Benchmark Metrics (existing fields) ===
if "Payment Amount*" in df.columns and "Benchmark_Payment_Amount_invoice_level" in df.columns:
    df["Invoice_Payment_Diff_vs_Benchmark_invoice_level"] = df["Payment Amount*"] - df["Benchmark_Payment_Amount_invoice_level"]

if "NRV Gap ($)" in df.columns:
    df["NRV_Gap_Dollar_invoice_level"] = df["NRV Gap ($)"]
if "NRV Gap (%)" in df.columns:
    df["NRV_Gap_Percent_invoice_level"] = df["NRV Gap (%)"]
if "NRV Gap Sum ($)" in df.columns:
    df["NRV_Gap_Sum_Dollar_invoice_level"] = df["NRV Gap Sum ($)"]

# === Step 7.5: DROP legacy duplicate columns from OUTPUT ===
for legacy_col in ["Revenue_Variance_$", "Revenue_Variance_%"]:
    if legacy_col in df.columns:
        df.drop(columns=[legacy_col], inplace=True)

# === Step 8: Export CSV and ZIP ===
df.to_csv(OUTPUT_CSV, index=False)
with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(OUTPUT_CSV, arcname=os.path.basename(OUTPUT_CSV))

print(f"✅ Enhanced file saved to: {OUTPUT_CSV}")
print(f"📦 Zipped file saved to: {OUTPUT_ZIP}")
print(f"ℹ️ Rows removed as totals: {rows_removed}")
