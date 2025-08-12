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
# Simple pipeline for web app (process uploaded files)
# ---------------------------------------------------
def process_uploaded_files():
    """Process uploaded files and generate basic outputs"""
    print("\n🚀 Starting simple file processing pipeline...")
    
    try:
        # Create a simple summary of uploaded files
        summary = {
            "uploaded_files": [f.name for f in uploaded_files],
            "file_count": len(uploaded_files),
            "total_size_mb": sum(f.stat().st_size for f in uploaded_files) / (1024 * 1024),
            "file_types": list(set(f.suffix.lower() for f in uploaded_files))
        }
        
        # Save summary to outputs
        summary_file = OUTPUTS_DIR / "upload_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"✅ Created upload summary: {summary_file}")
        
        # Import pandas safely
        try:
            import pandas as pd
            print("✅ Pandas imported successfully")
        except ImportError as e:
            print(f"❌ Pandas import failed: {e}")
            # Create a simple text summary instead
            summary_text = OUTPUTS_DIR / "upload_summary.txt"
            with open(summary_text, 'w') as f:
                f.write(f"Upload Summary\n")
                f.write(f"==============\n")
                f.write(f"Files: {', '.join(summary['uploaded_files'])}\n")
                f.write(f"Count: {summary['file_count']}\n")
                f.write(f"Total Size: {summary['total_size_mb']:.2f} MB\n")
                f.write(f"Types: {', '.join(summary['file_types'])}\n")
            print(f"✅ Created text summary: {summary_text}")
            return summary
        
        # Process each uploaded file with memory management
        for file_path in uploaded_files:
            if file_path.suffix.lower() in ['.csv', '.xlsx', '.xls']:
                try:
                    print(f"\n📊 Processing {file_path.name}...")
                    
                    # Check file size before processing
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    print(f"  - File size: {file_size_mb:.2f} MB")
                    
                    # For faster processing, use lower thresholds
                    if file_size_mb > 25:  # Reduced from 50MB
                        print(f"⚠️ File {file_path.name} is large ({file_size_mb:.2f} MB), creating summary only...")
                        
                        # For large files, just get basic info quickly
                        if file_path.suffix.lower() == '.csv':
                            # Count lines quickly without loading into memory
                            with open(file_path, 'r') as f:
                                line_count = sum(1 for _ in f)
                            
                            # Read just header for column info
                            df_sample = pd.read_csv(file_path, nrows=1)
                            total_rows = line_count - 1  # Subtract header
                        else:
                            # For Excel, read just first few rows
                            df_sample = pd.read_excel(file_path, nrows=5)
                            # Estimate total rows (this is approximate)
                            total_rows = len(df_sample) * 200  # Rough estimate
                        
                        print(f"  - Columns: {len(df_sample.columns)}")
                        print(f"  - Estimated total rows: {total_rows}")
                        
                        # Create a summary instead of full processing
                        summary_file = OUTPUTS_DIR / f"summary_{file_path.stem}.json"
                        file_summary = {
                            "filename": file_path.name,
                            "file_size_mb": file_size_mb,
                            "columns": list(df_sample.columns),
                            "estimated_total_rows": total_rows,
                            "note": "Large file - summary only for performance"
                        }
                        
                        with open(summary_file, 'w') as f:
                            json.dump(file_summary, f, indent=2)
                        
                        print(f"✅ Created summary for large file: {summary_file}")
                        
                    else:
                        # Process smaller files normally
                        print(f"  - Processing file completely...")
                        if file_path.suffix.lower() == '.csv':
                            df = pd.read_csv(file_path)
                        else:
                            df = pd.read_excel(file_path)
                        
                        # Create a simple processed version
                        processed_file = OUTPUTS_DIR / f"processed_{file_path.name}"
                        if file_path.suffix.lower() == '.csv':
                            df.to_csv(processed_file, index=False)
                        else:
                            df.to_excel(processed_file, index=False)
                        
                        print(f"✅ Processed {file_path.name} -> {processed_file.name}")
                        print(f"  - Rows: {len(df)}")
                        print(f"  - Columns: {len(df.columns)}")
                        
                        # Clear memory immediately
                        del df
                        
                except Exception as e:
                    print(f"⚠️ Could not process {file_path.name}: {e}")
                    traceback.print_exc()
                    continue
        
        return summary
        
    except Exception as e:
        print(f"❌ Error in process_uploaded_files: {e}")
        traceback.print_exc()
        raise

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
            "pipeline_version": "web_app_simple_v1"
        }
        
        with open(ARTIFACTS, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"\n🧾 Wrote artifact manifest: {ARTIFACTS}")
        print(f"📊 Generated {len(manifest['files'])} output files")
        
    except Exception as e:
        print(f"❌ Error creating artifact manifest: {e}")
        traceback.print_exc()

def main():
    print("🚀 Starting Revenue Performance Pipeline (Web App Version)")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"DATA_DIR   = {DATA_DIR}")
    print(f"UPLOADS_DIR= {UPLOADS_DIR}")
    print(f"OUTPUTS_DIR= {OUTPUTS_DIR}")
    print(f"LOGS_DIR   = {LOGS_DIR}")

    try:
        # Process uploaded files
        summary = process_uploaded_files()
        
        # Summarize outputs
        summarize_artifacts()
        
        print("\n🎉 Pipeline completed successfully!")
        print(f"📁 Check outputs in: {OUTPUTS_DIR}")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()