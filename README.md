# GridGuard — Electricity Consumption Anomaly & Investigation Priority Analytics

GridGuard is a Big Data Analytics platform for identifying unusual electricity-consumption patterns and prioritizing meters for investigation. It is designed as a utility-facing revenue-protection and investigation-support system, not as a direct electricity-theft classifier.

> **Important:** A high GridGuard score does not confirm electricity theft and is not a probability of theft. It represents the strength and persistence of analytical evidence that a meter's consumption behavior is abnormal and may deserve investigation.

## Overview

```text
Smart Meter Data
      ↓
Apache Spark
      ↓
Cleaning & Transformation
      ↓
Feature Engineering
      ↓
Temporal / Statistical / Peer Analysis
      ↓
Anomaly Detection
      ↓
Evidence Correlation
      ↓
Investigation Priority Score
      ↓
Gold Analytical Data
      ↓
PostgreSQL
      ↓
FastAPI
      ↓
React Dashboard
```

## Objectives

- Process large-scale smart-meter time-series data.
- Clean and validate electricity-consumption readings.
- Generate temporal, statistical and peer-based features.
- Detect unusual consumption patterns.
- Combine multiple analytical signals.
- Rank meters using an Investigation Priority Score.
- Serve analytical results through FastAPI.
- Visualize trends, anomalies, risk and investigation priority in React.
- Compare Pandas and Apache Spark scalability.

## Dataset

GridGuard uses the London Smart Meter Energy Consumption dataset published through the UK government data portal.

**Dataset scale used in the project:** approximately **167.9 million readings**, distributed across 168 CSV files.

Main fields:

- `LCLid` — meter identifier
- `stdorToU` — tariff type
- `DateTime` — timestamp
- `KWH/hh (per half hour)` — half-hourly consumption

Official dataset:
https://www.data.gov.uk/dataset/df8e55d4-c6d0-498e-abd8-f4c678766cbe/smartmeter-energy-consumption-data-in-london-households1

### Dataset limitation

The public dataset does not contain confirmed electricity-theft labels. Therefore, authentic data is used for consumption-pattern analysis, while controlled anomaly injection may be used only for secondary validation. Results must be described as anomaly/evidence analysis rather than confirmed theft detection.

## Technology Stack

### Big Data / Analytics
- Python
- Apache Spark / PySpark
- Pandas
- NumPy
- Scikit-learn
- Isolation Forest
- Statistical and temporal analysis
- Ensemble anomaly detection

### Backend
- FastAPI
- PostgreSQL
- Psycopg
- REST API

### Frontend
- React
- Vite
- Axios
- Recharts
- Lucide React
- CSS

### Development
- Git / GitHub
- Docker
- Java 17
- Hadoop 3.3.6 local support

## Architecture

```text
                    ┌─────────────────────────┐
                    │   Smart Meter Dataset   │
                    │      167.9M+ Records    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      Apache Spark       │
                    │ Ingestion / Cleaning /  │
                    │ Distributed Aggregation │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Feature Engineering   │
                    │ Temporal / Statistical  │
                    │ Peer-based Features     │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Anomaly Detection     │
                    │ Contextual + IF +       │
                    │ Ensemble Signals        │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │ Evidence Correlation &  │
                    │ Investigation Priority │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       PostgreSQL        │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │        FastAPI          │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      React Dashboard    │
                    └─────────────────────────┘
```

## Investigation Priority

GridGuard aggregates evidence such as:

- anomaly persistence
- anomaly frequency
- event severity
- recent abnormal activity
- model agreement
- historical deviation
- peer deviation
- contextual behavior

The resulting meter-level score is used to assign operational levels such as **Very High, High, Medium and Low**.

The score is an investigation-ranking mechanism. It does not establish theft, tampering or unauthorized activity.

## Project Structure

```text
GridGuard/
├── data/
│   ├── raw/
│   ├── sample/
│   ├── processed/
│   └── parquet/
│       ├── silver/
│       └── gold/
├── spark/
├── notebooks/
├── models/
├── backend/
│   └── app/
│       ├── main.py
│       ├── database.py
│       ├── models/
│       ├── schemas/
│       ├── routers/
│       └── services/
├── frontend/
│   └── src/
│       ├── components/
│       ├── pages/
│       ├── services/
│       ├── App.jsx
│       └── index.css
├── tests/
├── docs/
├── scripts/
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

## Local Setup

### Backend

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If `requirements.txt` is not ready yet:

```powershell
pip install fastapi "uvicorn[standard]" psycopg[binary] python-dotenv pandas "pandas<3.0" numpy scikit-learn pyspark==4.2.0 py4j==0.10.9.9
```

### PostgreSQL

Create the database:

```sql
CREATE DATABASE gridguard;
```

Create `.env` in the project root:

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/gridguard
```

Never commit `.env` or database credentials.

### Start FastAPI

From the project root:

```powershell
python -m uvicorn backend.app.main:app --reload
```

API: `http://127.0.0.1:8000`

Swagger: `http://127.0.0.1:8000/docs`

### Start React

In a second terminal:

```powershell
cd frontend
npm install
npm run dev
```

Frontend: `http://localhost:5173`

## Main API Endpoints

```text
GET /api/dashboard/summary
GET /api/meters/
GET /api/meters/{meter_id}
GET /api/meters/{meter_id}/consumption
GET /api/anomalies/
GET /api/anomalies/high-risk
GET /api/analytics/trends
GET /api/analytics/anomaly-types
GET /api/priorities/
GET /api/priorities/{meter_id}
GET /api/priorities/distribution/summary
```

## Database Tables

- `meters`
- `meter_aggregates`
- `anomalies`
- `risk_scores`
- `alerts`
- `daily_analysis`
- `meter_investigation_priority`

PostgreSQL is the serving/operational database. Apache Spark remains the main Big Data processing engine.

## Big Data Evaluation

The project compares Pandas and Apache Spark at increasing record counts:

| Records | Pandas (sec) | Spark (sec) |
|---:|---:|---:|
| 100K | 0.44 | 6.76 |
| 500K | 1.46 | 11.00 |
| 1M | 3.00 | 21.17 |
| 5M | 14.34 | 47.13 |
| 10M | 34.39 | 47.43 |

These timings are environment-specific. The purpose is to demonstrate the Big Data processing workflow and scalability characteristics, not to claim that Spark is faster than Pandas at every dataset size.

## Spark Development

Local development can use:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GridGuard")
    .master("local[2]")
    .getOrCreate()
)
```

Large-scale experiments can be executed in a higher-resource environment such as Kaggle.

## GitHub Publishing

Before publishing:

1. Do not commit `.venv/`.
2. Do not commit `.env` or passwords.
3. Do not commit the complete raw 7.9 GB dataset.
4. Do not commit generated Parquet/cache files.
5. Do not commit `node_modules/` or frontend builds.
6. Add only small sample data when necessary.

## Disclaimer

GridGuard is an analytical decision-support system. It identifies unusual electricity-consumption behavior and prioritizes meters for investigation. A high score does not prove electricity theft, meter tampering, unauthorized access or any other illegal activity. Final decisions require appropriate operational, technical and field evidence.

## License

Choose a license appropriate to your project and institutional requirements. MIT is a common option for academic software projects if the project ownership permits it.
