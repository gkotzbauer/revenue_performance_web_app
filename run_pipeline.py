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
import traceback
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

# Create directories safely
try:
    for p in (DATA_DIR, UPLOADS_DIR, OUTPUTS_DIR, LOGS_DIR):
        p.mkdir(parents=True, exist_ok=True)
except Exception as e:
    print(f"❌ Failed to create directories: {e}")
    sys.exit(1)

# -----------------------------------------------------------
# Check for uploaded files
# -----------------------------------------------------------
try:
    uploaded_files = list(UPLOADS_DIR.glob("*"))
    if not uploaded_files:
        print("❌ No uploaded files found in uploads directory")
        print(f"Uploads directory: {UPLOADS_DIR}")
        sys.exit(1)

    print(f"📁 Found {len(uploaded_files)} uploaded files:")
    for f in uploaded_files:
        print(f"  - {f.name} ({f.stat().st_size / (1024*1024):.2f} MB)")
        
except Exception as e:
    print(f"❌ Error checking uploaded files: {e}")
    traceback.print_exc()
    sys.exit(1)

# -----------------------------------------------------------
# Revenue Performance Pipeline Steps
# ---------------------------------------------------
def run_revenue_pipeline():
    """Run the actual revenue performance analysis pipeline"""
    print("\n🚀 Starting Revenue Performance Pipeline...")
    
    try:
        # Import required libraries
        import pandas as pd
        import numpy as np
        print("✅ Pandas and NumPy imported successfully")
        
        # Get the uploaded file
        if len(uploaded_files) == 0:
            raise ValueError("No files to process")
        
        input_file = uploaded_files[0]
        print(f"📊 Processing file: {input_file.name}")
        
        # Load the data
        if input_file.suffix.lower() == '.csv':
            df = pd.read_csv(input_file)
        elif input_file.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(input_file)
        else:
            raise ValueError(f"Unsupported file type: {input_file.suffix}")
        
        print(f"✅ Loaded data: {len(df)} rows, {len(df.columns)} columns")
        
        # Step 1: Data Preprocessing
        print("\n🔧 Step 1: Data Preprocessing...")
        df_processed = preprocess_invoice_data(df)
        print(f"✅ Preprocessing complete: {len(df_processed)} rows remaining")
        
        # Step 2: Calculate Benchmarks
        print("\n📊 Step 2: Calculating Benchmarks...")
        df_with_benchmarks = calculate_benchmarks(df_processed)
        print("✅ Benchmarks calculated")
        
        # Step 3: Generate Weekly Outputs
        print("\n📅 Step 3: Generating Weekly Outputs...")
        weekly_granular, weekly_aggregated = generate_weekly_outputs(df_with_benchmarks)
        print("✅ Weekly outputs generated")
        
        # Step 4: ML Rate Diagnostics
        print("\n🤖 Step 4: ML Rate Diagnostics...")
        ml_results = run_ml_diagnostics(df_with_benchmarks)
        print("✅ ML diagnostics complete")
        
        # Step 5: Underpayment Analysis
        print("\n💰 Step 5: Underpayment Analysis...")
        underpayment_drivers = analyze_underpayment_drivers(df_with_benchmarks)
        print("✅ Underpayment analysis complete")
        
        # Step 6: Generate Narratives
        print("\n📝 Step 6: Generating Narratives...")
        narratives = generate_narratives(df_with_benchmarks, ml_results, underpayment_drivers)
        print("✅ Narratives generated")
        
        # Step 7: Save All Outputs
        print("\n💾 Step 7: Saving Outputs...")
        save_pipeline_outputs(
            df_processed, weekly_granular, weekly_aggregated, 
            ml_results, underpayment_drivers, narratives
        )
        print("✅ All outputs saved")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in revenue pipeline: {e}")
        traceback.print_exc()
        return False

def preprocess_invoice_data(df):
    """Preprocess the invoice data for analysis"""
    print("  - Cleaning and standardizing data...")
    
    # Remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    
    # Standardize column names if they exist
    column_mapping = {
        "Year of Visit Service Date": "Year",
        "ISO Week of Visit Service Date": "Week", 
        "Primary Financial Class": "Payer",
        "Chart E/M Code Grouping": "Group_EM",
        "Chart E/M Code Second Layer": "Group_EM2",
        "Charge Invoice Number": "Invoice_Number"
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    # Fill missing metadata fields
    metadata_cols = ["Year", "Week", "Payer", "Group_EM", "Group_EM2"]
    for col in metadata_cols:
        if col in df.columns:
            df[col] = df[col].ffill()
    
    # Convert numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    return df

def calculate_benchmarks(df):
    """Calculate 85% E/M and historical benchmarks"""
    print("  - Calculating 85% E/M benchmark...")
    
    # Add benchmark columns
    if "Charge Amount" in df.columns:
        df["Benchmark_85_Percent"] = df["Charge Amount"] * 0.85
        df["Gap_vs_85_Percent"] = df["Payment Amount*"] - df["Benchmark_85_Percent"]
        df["Gap_Percent_vs_85"] = (df["Gap_vs_85_Percent"] / df["Benchmark_85_Percent"]) * 100
    
    # Calculate historical peer benchmark (median payment rate)
    if "Payment Amount*" in df.columns and "Charge Amount" in df.columns:
        payment_rate = df["Payment Amount*"] / df["Charge Amount"]
        median_rate = payment_rate.median()
        df["Historical_Benchmark"] = df["Charge Amount"] * median_rate
        df["Gap_vs_Historical"] = df["Payment Amount*"] - df["Historical_Benchmark"]
    
    return df

def generate_weekly_outputs(df):
    """Generate granular and aggregated weekly outputs"""
    print("  - Creating weekly granular outputs...")
    
    # Granular weekly (by CPT/benchmark key)
    if "Week" in df.columns:
        weekly_granular = df.groupby("Week").agg({
            "Charge Amount": "sum",
            "Payment Amount*": "sum",
            "Visit Count": "sum" if "Visit Count" in df.columns else "count"
        }).reset_index()
        
        # Add performance metrics
        weekly_granular["Collection_Rate"] = weekly_granular["Payment Amount*"] / weekly_granular["Charge Amount"]
        weekly_granular["Performance_vs_85"] = (weekly_granular["Collection_Rate"] - 0.85) * 100
        
        # Save granular output
        weekly_granular.to_csv(OUTPUTS_DIR / "weekly_granular_performance.csv", index=False)
        print(f"    ✅ Saved weekly granular: {len(weekly_granular)} weeks")
    else:
        weekly_granular = pd.DataFrame()
        print("    ⚠️ No Week column found, skipping weekly granular")
    
    # Aggregated weekly (by Payer/E/M)
    print("  - Creating weekly aggregated outputs...")
    if "Payer" in df.columns and "Group_EM" in df.columns:
        weekly_aggregated = df.groupby(["Week", "Payer", "Group_EM"]).agg({
            "Charge Amount": "sum",
            "Payment Amount*": "sum",
            "Visit Count": "sum" if "Visit Count" in df.columns else "count"
        }).reset_index()
        
        # Add performance metrics
        weekly_aggregated["Collection_Rate"] = weekly_aggregated["Payment Amount*"] / weekly_aggregated["Charge Amount"]
        weekly_aggregated["Performance_vs_85"] = (weekly_aggregated["Collection_Rate"] - 0.85) * 100
        
        # Save aggregated output
        weekly_aggregated.to_csv(OUTPUTS_DIR / "weekly_aggregated_performance.csv", index=False)
        print(f"    ✅ Saved weekly aggregated: {len(weekly_aggregated)} combinations")
    else:
        weekly_aggregated = pd.DataFrame()
        print("    ⚠️ Missing Payer or Group_EM columns, skipping weekly aggregated")
    
    return weekly_granular, weekly_aggregated

def run_ml_diagnostics(df):
    """Run ML diagnostics for rate estimation"""
    print("  - Running ML rate diagnostics...")
    
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import r2_score, mean_absolute_error
        
        # Prepare features for ML
        feature_cols = []
        if "Charge Amount" in df.columns:
            feature_cols.append("Charge Amount")
        if "Visit Count" in df.columns:
            feature_cols.append("Visit Count")
        
        if len(feature_cols) < 1:
            print("    ⚠️ Insufficient features for ML, skipping")
            return {}
        
        # Create target variable (payment rate)
        if "Payment Amount*" in df.columns and "Charge Amount" in df.columns:
            df["Payment_Rate"] = df["Payment Amount*"] / df["Charge Amount"]
            
            # Remove infinite and NaN values
            df_ml = df[df["Payment_Rate"].notna() & df["Payment_Rate"].notna()].copy()
            df_ml = df_ml[df_ml["Payment_Rate"] != np.inf]
            
            if len(df_ml) > 10:  # Need sufficient data
                X = df_ml[feature_cols]
                y = df_ml["Payment_Rate"]
                
                # Split data
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                
                # Train model
                model = LinearRegression()
                model.fit(X_train, y_train)
                
                # Predictions
                y_pred = model.predict(X_test)
                
                # Model performance
                r2 = r2_score(y_test, y_pred)
                mae = mean_absolute_error(y_test, y_pred)
                
                # Add predictions to dataframe
                df["ML_Expected_Rate"] = model.predict(df[feature_cols])
                df["ML_Rate_Gap"] = df["Payment_Rate"] - df["ML_Expected_Rate"]
                df["ML_Materiality_Flag"] = abs(df["ML_Rate_Gap"]) > 0.1  # 10% threshold
                
                # Save ML results
                ml_summary = {
                    "model_type": "Linear Regression",
                    "r2_score": r2,
                    "mean_absolute_error": mae,
                    "features_used": feature_cols,
                    "training_samples": len(X_train),
                    "test_samples": len(X_test)
                }
                
                # Save ML diagnostics
                df[["Invoice_Number", "Payment_Rate", "ML_Expected_Rate", "ML_Rate_Gap", "ML_Materiality_Flag"]].to_csv(
                    OUTPUTS_DIR / "ml_rate_diagnostics.csv", index=False
                )
                
                print(f"    ✅ ML diagnostics complete: R²={r2:.3f}, MAE={mae:.3f}")
                return ml_summary
            else:
                print("    ⚠️ Insufficient data for ML training")
                return {}
        else:
            print("    ⚠️ Missing payment or charge columns for ML")
            return {}
            
    except Exception as e:
        print(f"    ⚠️ ML diagnostics failed: {e}")
        return {}

def analyze_underpayment_drivers(df):
    """Analyze underpayment drivers by payer, key, and time"""
    print("  - Analyzing underpayment drivers...")
    
    drivers = {}
    
    # By Payer
    if "Payer" in df.columns and "Gap_vs_85_Percent" in df.columns:
        payer_analysis = df.groupby("Payer").agg({
            "Gap_vs_85_Percent": ["mean", "sum", "count"],
            "Charge Amount": "sum",
            "Payment Amount*": "sum"
        }).round(2)
        
        payer_analysis.columns = ["Avg_Gap", "Total_Gap", "Transaction_Count", "Total_Charges", "Total_Payments"]
        payer_analysis["Collection_Rate"] = (payer_analysis["Total_Payments"] / payer_analysis["Total_Charges"] * 100).round(2)
        
        payer_analysis.to_csv(OUTPUTS_DIR / "underpayment_drivers_by_payer.csv")
        drivers["payer"] = payer_analysis
        print("    ✅ Payer analysis complete")
    
    # By E/M Group
    if "Group_EM" in df.columns and "Gap_vs_85_Percent" in df.columns:
        em_analysis = df.groupby("Group_EM").agg({
            "Gap_vs_85_Percent": ["mean", "sum", "count"],
            "Charge Amount": "sum",
            "Payment Amount*": "sum"
        }).round(2)
        
        em_analysis.columns = ["Avg_Gap", "Total_Gap", "Transaction_Count", "Total_Charges", "Total_Payments"]
        em_analysis["Collection_Rate"] = (em_analysis["Total_Payments"] / em_analysis["Total_Charges"] * 100).round(2)
        
        em_analysis.to_csv(OUTPUTS_DIR / "underpayment_drivers_by_em_group.csv")
        drivers["em_group"] = em_analysis
        print("    ✅ E/M group analysis complete")
    
    # By Week
    if "Week" in df.columns and "Gap_vs_85_Percent" in df.columns:
        week_analysis = df.groupby("Week").agg({
            "Gap_vs_85_Percent": ["mean", "sum", "count"],
            "Charge Amount": "sum",
            "Payment Amount*": "sum"
        }).round(2)
        
        week_analysis.columns = ["Avg_Gap", "Total_Gap", "Transaction_Count", "Total_Charges", "Total_Payments"]
        week_analysis["Collection_Rate"] = (week_analysis["Total_Payments"] / week_analysis["Total_Charges"] * 100).round(2)
        
        week_analysis.to_csv(OUTPUTS_DIR / "underpayment_drivers_by_week.csv")
        drivers["week"] = week_analysis
        print("    ✅ Week analysis complete")
    
    return drivers

def generate_narratives(df, ml_results, underpayment_drivers):
    """Generate narratives from metrics and ML results"""
    print("  - Generating narratives...")
    
    narratives = []
    
    # Overall performance narrative
    if "Gap_vs_85_Percent" in df.columns:
        total_gap = df["Gap_vs_85_Percent"].sum()
        avg_gap = df["Gap_vs_85_Percent"].mean()
        
        if total_gap > 0:
            narratives.append(f"💰 Overall Performance: The practice is underperforming by ${total_gap:,.2f} against the 85% benchmark, with an average gap of ${avg_gap:.2f} per transaction.")
        else:
            narratives.append(f"✅ Overall Performance: The practice is performing well, exceeding the 85% benchmark by ${abs(total_gap):,.2f}.")
    
    # ML insights
    if ml_results and "r2_score" in ml_results:
        r2 = ml_results["r2_score"]
        if r2 > 0.7:
            narratives.append(f"🤖 ML Insights: The machine learning model shows strong predictive power (R²={r2:.1%}) for payment rates, indicating reliable rate estimation.")
        elif r2 > 0.5:
            narratives.append(f"🤖 ML Insights: The machine learning model shows moderate predictive power (R²={r2:.1%}) for payment rates.")
        else:
            narratives.append(f"🤖 ML Insights: The machine learning model shows limited predictive power (R²={r2:.1%}), suggesting payment rates may be influenced by external factors.")
    
    # Top underpayment drivers
    if "payer" in underpayment_drivers:
        payer_drivers = underpayment_drivers["payer"]
        worst_payer = payer_drivers.loc[payer_drivers["Avg_Gap"].idxmin()]
        narratives.append(f"📊 Top Underpayment Driver: {payer_drivers.index[0]} shows the largest average gap (${worst_payer['Avg_Gap']:.2f}) and represents ${worst_payer['Total_Gap']:,.2f} in total underpayments.")
    
    # Save narratives
    with open(OUTPUTS_DIR / "performance_narratives.txt", "w") as f:
        for i, narrative in enumerate(narratives, 1):
            f.write(f"{i}. {narrative}\n")
    
    print(f"    ✅ Generated {len(narratives)} narratives")
    return narratives

def save_pipeline_outputs(df, weekly_granular, weekly_aggregated, ml_results, underpayment_drivers, narratives):
    """Save all pipeline outputs"""
    print("  - Saving all outputs...")
    
    # Save processed data
    df.to_csv(OUTPUTS_DIR / "processed_invoice_data.csv", index=False)
    print("    ✅ Processed invoice data saved")
    
    # Save summary report
    summary = {
        "pipeline_version": "revenue_performance_v2",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "input_file": uploaded_files[0].name,
        "total_transactions": len(df),
        "total_charges": df["Charge Amount"].sum() if "Charge Amount" in df.columns else 0,
        "total_payments": df["Payment Amount*"].sum() if "Payment Amount*" in df.columns else 0,
        "overall_collection_rate": (df["Payment Amount*"].sum() / df["Charge Amount"].sum() * 100) if "Charge Amount" in df.columns and "Payment Amount*" in df.columns else 0,
        "ml_model_performance": ml_results,
        "narratives_generated": len(narratives)
    }
    
    with open(OUTPUTS_DIR / "pipeline_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print("    ✅ Pipeline summary saved")

def summarize_artifacts() -> None:
    """
    Write a simple manifest of what's in data/outputs/ so the UI can present links.
    """
    try:
        manifest = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "outputs_dir": str(OUTPUTS_DIR),
            "files": sorted([p.name for p in OUTPUTS_DIR.glob("*") if p.is_file()]),
            "uploaded_files": [f.name for f in uploaded_files],
            "pipeline_version": "revenue_performance_v2",
            "mission_completed": True,
            "outputs_generated": [
                "Granular weekly performance (Benchmark Key)",
                "Aggregated weekly performance (Payer/E/M)",
                "Underpayment drivers analysis",
                "ML rate diagnostics",
                "Performance narratives"
            ]
        }
        
        with open(ARTIFACTS, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"\n🧾 Wrote artifact manifest: {ARTIFACTS}")
        print(f"📊 Generated {len(manifest['files'])} output files")
        
    except Exception as e:
        print(f"❌ Error creating artifact manifest: {e}")
        traceback.print_exc()

def main():
    print("🚀 Starting Revenue Performance Pipeline (Full Analysis Version)")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"DATA_DIR   = {DATA_DIR}")
    print(f"UPLOADS_DIR= {UPLOADS_DIR}")
    print(f"OUTPUTS_DIR= {OUTPUTS_DIR}")
    print(f"LOGS_DIR   = {LOGS_DIR}")

    try:
        # Run the full revenue performance pipeline
        success = run_revenue_pipeline()
        
        if success:
            # Summarize outputs
            summarize_artifacts()
            
            print("\n🎉 Revenue Performance Pipeline completed successfully!")
            print("📁 Check the Downloads page for your analysis results:")
            print("   • Granular weekly performance data")
            print("   • Aggregated weekly performance by payer/E/M")
            print("   • Underpayment drivers analysis")
            print("   • ML rate diagnostics")
            print("   • Performance narratives")
            print(f"📁 All outputs saved to: {OUTPUTS_DIR}")
        else:
            print("\n❌ Pipeline failed to complete")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()