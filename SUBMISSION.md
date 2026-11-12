# Assignment A: Interaction Ledger & Reporting Pipeline - Final Submission

## Project Summary
This project implements a robust interaction ledger and reporting pipeline designed to ingest, validate, and process customer engagement data drops. It handles data cleaning, schema validation, mapping of workflows and dispositions, and generates comprehensive operational reports into a centralized database (`pipeline.db`).

## Repository Architecture & File Structure
- `pipeline.py`: Core pipeline engine handling database initialization, schema loading, data ingestion, and analytical report generation.
- `test_pipeline.py`: Comprehensive test suite utilizing `pytest` to validate core logic, data transformations, and normalization pipelines.
- `requirements.txt`: Configuration file specifying necessary Python packages (`pandas`, `psycopg2-binary`, `pytest`).
- `assignment-a-data/`: Directory housing raw workflow schemas, disposition mappings, and data generation utilities.
- `reports/`: Output directory containing generated comma-separated value (`.csv`) summaries and data quality metrics (`.json`).

## Execution Instructions

1. **Install Dependencies:**
   Ensure all required Python packages are installed in your environment:
   ```bash
   pip install -r requirements.txt