# Weather Lakehouse Pipeline

End-to-end Data Engineering pipeline using Open-Meteo API, Apache Airflow, AWS S3 Tables, AWS Glue, and Amazon Athena implementing Medallion Architecture (Bronze → Silver → Gold).

## Architecture
![Architecture](docs/architecture.png)

## Tech Stack
- Apache Airflow (Docker) — Orchestration
- Open-Meteo API — Data Source (Free, no API key)
- Amazon S3 + S3 Tables — Storage + Iceberg
- AWS Glue — Transformation (PySpark)
- Amazon Athena — Querying + Gold MERGE

## Layers
| Layer | Format | Purpose |
|---|---|---|
| Bronze | JSON | Raw API response, partitioned by city + date |
| Silver | Parquet (Iceberg) | Cleaned, typed, flattened |
| Gold | Parquet (Iceberg) | Aggregated, analysis-ready |

## Setup
Copy `.env.example` to `.env` and fill in your AWS credentials.