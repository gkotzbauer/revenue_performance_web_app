# Revenue Performance Web App

## Overview
This application analyzes healthcare revenue cycle performance using a multi-step pipeline with both traditional and machine learning (ML)-based diagnostics.  
It includes:
- Invoice-level preprocessing
- Benchmark enhancement
- Weekly performance summaries
- Underpayment detection & drivers
- CPT-level rate analysis
- ML-based predictive modeling
- Automated narrative generation

The app has a React front-end for uploading data, viewing results, and accessing ML diagnostics.

---

## Features
- **Full Revenue Performance Pipeline** — 8 backend steps + master runner.
- **Machine Learning** — HistGradientBoostingRegressor with rate gap diagnostics.
- **Narratives** — Automatically generated 'What Went Well' and 'What Can Be Improved' text.
- **Zero-Balance Collection Analysis** — Detects payer-level collection issues.
- **Front-End Dashboard** — Upload data, view results, and access a "Help / ML Guide".

---

## Installation

### Backend (Python)
```bash
cd backend
pip install -r ../requirements.txt
