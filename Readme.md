# Indian Railways TRC Analytics Platform

A comprehensive, automated data pipeline and analytics web application designed for processing, storing, and analyzing Indian Railways TRC (Track Recording Car) data. This project seamlessly ingests multiple file formats (DOCX, XLSX), normalizes the data into a scalable one-table-per-file SQLite architecture, and provides a powerful web-based UI for insightful analytics.

## 🚀 Features

- **Automated Data Ingestion:** Recursively scans and processes nested directories of `.xlsx` and `.docx` files.
- **DOCX to XLSX Conversion:** Built-in conversion layer that structured extracts inspection data from Word documents.
- **Scalable Architecture:** Implements a dynamic one-file-per-table schema in SQLite to handle thousands of individual run datasets without performance degradation.
- **Deduplication & Caching:** Intelligent file discovery and hashing to prevent duplicate processing and maintain directory hygiene.
- **Null Byte Handling:** Robust data sanitization ensuring data integrity before database insertion.
- **Advanced Analytics Dashboard:** A Flask-based web interface to query insights such as:
  - Worst 20% track locations.
  - Resource deployment metrics.
  - False alert detection.
  - Consecutive defect tracking.
  - Repeated defect locations across multiple runs.

## 📁 Project Structure

```
├── app.py                      # Main Flask web application and analytics engine
├── railway_pipeline.py         # Core data ingestion and ETL pipeline
├── docx_to_xlsx_converter.py   # Utility for converting unstructured DOCX to structured XLSX
├── inspect_db.py               # CLI tool for database integrity checks and reporting
├── railway.db                  # Primary SQLite database (Auto-generated after first run)
├── data/                       # Directory containing raw track data (.xlsx, .docx)
└── README.md                   # Project documentation
```

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.8+
- Required Python libraries: `flask`, `pandas`, `openpyxl`

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd IndianRailwaysProject_Second_Data
   ```

2. **Install dependencies:**
   ```bash
   pip install flask pandas openpyxl
   ```

## 🖥️ Usage

### Running the Web Application
Start the Flask server to access the analytics dashboard:
```bash
python app.py
```
*Open your browser and navigate to: `http://localhost:5000`*

### Ingesting Data
The web interface allows you to trigger the pipeline directly. Alternatively, you can run the pipeline from the command line:
```bash
python railway_pipeline.py ./data ./railway.db
```

### Inspecting the Database
To verify table generation, lineage, and data integrity:
```bash
python inspect_db.py
```

## 🏗️ Architecture Notes

- **Dynamic Table Discovery:** The application dynamically queries available tables based on a registry pattern (`processed_files`), enabling robust data merging (`UNION`) on the fly for advanced analytics.
- **Streaming Logs:** The pipeline execution pushes real-time event logs to the web frontend using Server-Sent Events (SSE).

## 📝 Recent Updates
- Refactored pipeline to support recursive directory scanning and MDB file integration strategies.
- Transitioned to the `one-file-per-table` schema for improved query modularity.
- Automated document layout checks and embedded image label parsing.
