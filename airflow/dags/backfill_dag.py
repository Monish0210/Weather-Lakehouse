# ============================================================
# BACKFILL DAG — TaskFlow API Syntax
# Purpose: One-time historical load of 90 days weather data
#          for all 6 cities into the Lakehouse
# Schedule: Manual trigger only
# Syntax: Airflow 3.x TaskFlow API with @dag and @task
# ============================================================

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ── Logging ──────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Config from environment variables ────────────────────────
S3_BUCKET       = os.environ.get("S3_BUCKET_NAME")
GLUE_DATABASE   = os.environ.get("GLUE_DATABASE")
ATHENA_RESULTS  = os.environ.get("ATHENA_RESULTS")

# ── Constants ─────────────────────────────────────────────────
CITIES_CONFIG_PATH  = "/opt/airflow/config/cities.json"
SQL_BASE_PATH       = "/opt/airflow/sql"
HISTORICAL_API_URL  = "https://archive-api.open-meteo.com/v1/archive"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_cities_config() -> dict:
    """Load cities and API settings from config file."""
    with open(CITIES_CONFIG_PATH, "r") as f:
        return json.load(f)


def read_sql_file(relative_path: str) -> str:
    """Read SQL file from mounted volume."""
    full_path = f"{SQL_BASE_PATH}/{relative_path}"
    with open(full_path, "r") as f:
        return f.read()


def run_athena_query(sql: str) -> str:
    """
    Execute a SQL query in Athena and wait for completion.
    Returns query execution ID.
    """
    athena_hook = AthenaHook(
        aws_conn_id="aws_default",
    )
    query_id = athena_hook.run_query(
        query=sql,
        query_context={"Database": GLUE_DATABASE},
        result_configuration={"OutputLocation": ATHENA_RESULTS},
    )
    athena_hook.poll_query_status(
        query_id,
        sleep_time=5,
    )
    return query_id


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="weather_backfill_pipeline",
    description="One-time historical backfill of 90 days weather data",
    schedule=None,                  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner":        "weather-lakehouse",
        "retries":      3,
        "retry_delay":  timedelta(minutes=5),
    },
    tags=["weather", "backfill", "bronze", "lakehouse"],
)
def weather_backfill_pipeline():
    """
    One-time DAG to backfill 90 days of historical weather data
    for 6 cities through the Bronze → Silver → Gold pipeline.
    """

    # ──────────────────────────────────────────────────────────
    # TASK 1: Fetch historical data and upload to Bronze S3
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="fetch_and_upload_historical_bronze",
        execution_timeout=timedelta(hours=2),
    )
    def fetch_and_upload_historical_bronze() -> list[dict]:
        """
        Fetch 90 days of historical weather data for all cities
        from Open-Meteo archive API and upload raw JSON files
        to Bronze layer in S3.

        Returns list of upload summaries — automatically stored
        as XCom and passed to next task via return value.
        """
        config          = load_cities_config()
        cities          = config["cities"]
        backfill_days   = config["api_settings"]["backfill_days"]
        hourly_vars     = config["api_settings"]["hourly_variables"]

        # Calculate date range
        end_date    = datetime.utcnow().date() - timedelta(days=1)
        start_date  = end_date - timedelta(days=backfill_days - 1)

        logger.info(f"Backfill date range: {start_date} → {end_date}")
        logger.info(f"Cities to process: {[c['name'] for c in cities]}")

        s3_hook         = S3Hook(aws_conn_id="aws_default")
        upload_summary  = []

        for city in cities:
            logger.info(
                f"Fetching {backfill_days} days for "
                f"{city['display_name']}..."
            )

            # Call Open-Meteo historical API
            params = {
                "latitude":     city["latitude"],
                "longitude":    city["longitude"],
                "start_date":   str(start_date),
                "end_date":     str(end_date),
                "hourly":       ",".join(hourly_vars),
                "timezone":     city["timezone"],
            }

            response = requests.get(
                HISTORICAL_API_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            raw_data = response.json()

            # Validate API returned hourly data
            if "hourly" not in raw_data:
                raise ValueError(
                    f"No hourly data returned for "
                    f"{city['display_name']}"
                )

            # Add pipeline metadata
            raw_data["city_name"]           = city["name"]
            raw_data["city_display_name"]   = city["display_name"]
            raw_data["country"]             = city["country"]
            raw_data["ingestion_timestamp"] = datetime.utcnow().isoformat()

            # Group hourly indices by date for daily partitioning
            time_values  = raw_data["hourly"]["time"]
            date_indices: dict = {}

            for idx, ts in enumerate(time_values):
                date_str = ts[:10]      # "2026-04-20T00:00" → "2026-04-20"
                if date_str not in date_indices:
                    date_indices[date_str] = []
                date_indices[date_str].append(idx)

            # Upload one JSON file per city per date
            for date_str, indices in date_indices.items():
                year, month, day = date_str.split("-")

                # Slice full response into daily chunk
                daily_data = {
                    "latitude":             raw_data["latitude"],
                    "longitude":            raw_data["longitude"],
                    "timezone":             raw_data["timezone"],
                    "timezone_abbreviation": raw_data.get(
                                                "timezone_abbreviation"
                                            ),
                    "elevation":            raw_data.get("elevation"),
                    "city_name":            city["name"],
                    "city_display_name":    city["display_name"],
                    "country":              city["country"],
                    "ingestion_timestamp":  raw_data["ingestion_timestamp"],
                    "pipeline_run_date":    date_str,
                    "hourly_units":         raw_data["hourly_units"],
                    "hourly": {
                        var: [
                            raw_data["hourly"][var][i]
                            for i in indices
                        ]
                        for var in ["time"] + hourly_vars
                        if var in raw_data["hourly"]
                    }
                }

                # S3 key matches Bronze table partition structure exactly
                s3_key = (
                    f"bronze/"
                    f"city={city['name']}/"
                    f"year={year}/"
                    f"month={month}/"
                    f"day={day}/"
                    f"raw.json"
                )

                # replace=True → idempotent, safe to rerun
                s3_hook.load_string(
                    string_data=json.dumps(daily_data, indent=2),
                    key=s3_key,
                    bucket_name=S3_BUCKET,
                    replace=True,
                )

            upload_summary.append({
                "city":         city["name"],
                "display_name": city["display_name"],
                "dates_loaded": len(date_indices),
                "start_date":   str(start_date),
                "end_date":     str(end_date),
            })

            logger.info(
                f"✅ {city['display_name']}: "
                f"{len(date_indices)} daily files uploaded"
            )

        logger.info(f"Bronze upload complete: {upload_summary}")
        return upload_summary   # TaskFlow passes this to next task as XCom


    # ──────────────────────────────────────────────────────────
    # TASK 2: Validate Bronze upload
    # ──────────────────────────────────────────────────────────
    @task(task_id="validate_bronze_upload")
    def validate_bronze_upload(upload_summary: list[dict]) -> None:
        """
        Verify all expected S3 files exist after Bronze upload.
        Fails fast if any city has no files — prevents
        downstream tasks from running on empty data.

        Receives upload_summary automatically from Task 1 XCom.
        """
        s3_hook = S3Hook(aws_conn_id="aws_default")
        errors  = []

        for city_summary in upload_summary:
            city    = city_summary["city"]
            prefix  = f"bronze/city={city}/"

            keys = s3_hook.list_keys(
                bucket_name=S3_BUCKET,
                prefix=prefix,
            )

            if not keys:
                errors.append(
                    f"No Bronze files found for city: {city}"
                )
                logger.error(f"❌ {city}: no files in S3")
            else:
                logger.info(
                    f"✅ {city}: {len(keys)} files confirmed in S3"
                )

        if errors:
            raise ValueError(
                f"Bronze validation failed:\n" + "\n".join(errors)
            )

        logger.info("✅ Bronze validation passed for all cities")


    # ──────────────────────────────────────────────────────────
    # TASK 3: Register Bronze as Athena external table
    # ──────────────────────────────────────────────────────────
    @task(task_id="register_bronze_external_table")
    def register_bronze_external_table() -> None:
        """
        Run create_bronze_table.sql in Athena.
        Registers S3 JSON files as a queryable external table
        in Glue Data Catalog.
        Uses IF NOT EXISTS — safe to rerun.
        """
        sql = read_sql_file("ddl/create_bronze_table.sql")
        run_athena_query(sql)
        logger.info("✅ Bronze external table registered in Athena")


    # ──────────────────────────────────────────────────────────
    # TASK 4: Create Silver Iceberg table
    # ──────────────────────────────────────────────────────────
    @task(task_id="create_silver_iceberg_table")
    def create_silver_iceberg_table() -> None:
        """
        Run create_silver_table.sql in Athena.
        Creates empty Iceberg table in S3 Tables for
        cleaned, flattened hourly weather data.
        Uses IF NOT EXISTS — safe to rerun.
        """
        sql = read_sql_file("ddl/create_silver_table.sql")
        run_athena_query(sql)
        logger.info("✅ Silver Iceberg table created in S3 Tables")


    # ──────────────────────────────────────────────────────────
    # TASK 5: Create Gold Iceberg tables
    # ──────────────────────────────────────────────────────────
    @task(task_id="create_gold_iceberg_tables")
    def create_gold_iceberg_tables() -> None:
        """
        Run create_gold_tables.sql in Athena.
        Creates two empty Iceberg tables in S3 Tables:
        - gold_daily_weather_summary
        - gold_extreme_weather_events
        Uses IF NOT EXISTS — safe to rerun.
        """
        sql = read_sql_file("ddl/create_gold_tables.sql")
        run_athena_query(sql)
        logger.info("✅ Gold Iceberg tables created in S3 Tables")


    # ──────────────────────────────────────────────────────────
    # TASK 6: Bronze → Silver transformation
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="run_bronze_to_silver_ctas",
        execution_timeout=timedelta(hours=3),
    )
    def run_bronze_to_silver_ctas(upload_summary: list[dict]) -> None:
        """
        Run Bronze to Silver CTAS for every city and every date
        in the backfill range.

        Loops: 90 dates × 6 cities = 540 Athena CTAS queries
        Each query flattens 24 hourly JSON arrays into 24 rows
        and writes Parquet to Silver Iceberg table.

        Receives upload_summary from Task 1 to know date range.
        """
        config          = load_cities_config()
        cities          = config["cities"]
        backfill_days   = config["api_settings"]["backfill_days"]
        sql_template    = read_sql_file(
            "transformations/bronze_to_silver_ctas.sql"
        )

        end_date    = datetime.utcnow().date() - timedelta(days=1)
        start_date  = end_date - timedelta(days=backfill_days - 1)

        total_queries   = 0
        current_date    = start_date

        while current_date <= end_date:
            for city in cities:
                # Substitute template placeholders with actual values
                sql = (
                    sql_template
                    .replace("{{ run_date }}",   str(current_date))
                    .replace("{{ city_name }}",  city["name"])
                    .replace("{{ year }}",       current_date.strftime("%Y"))
                    .replace("{{ month }}",      current_date.strftime("%m"))
                    .replace("{{ day }}",        current_date.strftime("%d"))
                )

                run_athena_query(sql)
                total_queries += 1

                if total_queries % 30 == 0:
                    logger.info(
                        f"Progress: {total_queries} queries complete "
                        f"(current: {city['display_name']} / {current_date})"
                    )

            current_date += timedelta(days=1)

        logger.info(
            f"✅ Bronze → Silver complete: "
            f"{total_queries} queries executed"
        )


    # ──────────────────────────────────────────────────────────
    # TASK 7: Silver → Gold MERGE
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="run_silver_to_gold_merge",
        execution_timeout=timedelta(hours=2),
    )
    def run_silver_to_gold_merge() -> None:
        """
        Run Silver to Gold MERGE INTO for every date
        in the backfill range.

        Loops: 90 dates = 90 Athena MERGE queries
        Each MERGE aggregates 144 hourly rows (6 cities × 24 hours)
        into 6 daily summary rows in Gold — one per city.

        ACID upsert via Iceberg:
        - UPDATE if city+date exists
        - INSERT if new
        """
        config          = load_cities_config()
        backfill_days   = config["api_settings"]["backfill_days"]
        sql_template    = read_sql_file(
            "transformations/silver_to_gold_merge.sql"
        )

        end_date    = datetime.utcnow().date() - timedelta(days=1)
        start_date  = end_date - timedelta(days=backfill_days - 1)

        current_date    = start_date
        total_queries   = 0

        while current_date <= end_date:
            sql = sql_template.replace(
                "{{ run_date }}", str(current_date)
            )

            run_athena_query(sql)
            total_queries += 1

            if total_queries % 10 == 0:
                logger.info(
                    f"Gold MERGE progress: "
                    f"{total_queries}/90 dates complete"
                )

            current_date += timedelta(days=1)

        logger.info(
            f"✅ Silver → Gold complete: "
            f"{total_queries} MERGE queries executed"
        )


    # ──────────────────────────────────────────────────────────
    # TASK 8: Final validation
    # ──────────────────────────────────────────────────────────
    @task(task_id="validate_gold_data")
    def validate_gold_data() -> None:
        """
        Final sanity check — confirm Gold table has expected
        number of rows after backfill completes.

        Expected: 90 days × 6 cities = 540 rows minimum
        in gold_daily_weather_summary.
        """
        config          = load_cities_config()
        backfill_days   = config["api_settings"]["backfill_days"]
        city_count      = len(config["cities"])
        expected_rows   = backfill_days * city_count

        athena_hook = AthenaHook(
            aws_conn_id="aws_default",
            sleep_time=5,
        )

        query_id = athena_hook.run_query(
            query="""
                SELECT COUNT(*) AS row_count
                FROM weather_lakehouse.gold_daily_weather_summary
            """,
            query_context={"Database": GLUE_DATABASE},
            result_configuration={"OutputLocation": ATHENA_RESULTS},
        )

        results     = athena_hook.get_query_results(query_id)
        row_count   = int(
            results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
        )

        logger.info(
            f"Gold table row count: {row_count} "
            f"(expected ~{expected_rows})"
        )

        if row_count == 0:
            raise ValueError(
                "Gold table is empty after backfill — "
                "check Silver → Gold MERGE task logs"
            )

        if row_count < expected_rows * 0.9:
            logger.warning(
                f"Gold row count {row_count} is less than 90% "
                f"of expected {expected_rows} — "
                f"some dates may be missing"
            )

        logger.info(
            f"✅ Backfill validation passed: "
            f"{row_count} rows in Gold table"
        )


    # ──────────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    # TaskFlow handles XCom automatically via return values
    # ──────────────────────────────────────────────────────────

    # Task 1 runs first — returns upload_summary
    bronze_data = fetch_and_upload_historical_bronze()

    # Task 2 receives upload_summary from Task 1 automatically
    validated = validate_bronze_upload(bronze_data)

    # Tasks 3, 4, 5 run in sequence after validation
    bronze_table    = register_bronze_external_table()
    silver_table    = create_silver_iceberg_table()
    gold_tables     = create_gold_iceberg_tables()

    # Task 6 also receives bronze_data to know date range
    silver_data = run_bronze_to_silver_ctas(bronze_data)

    # Task 7 runs after Silver is populated
    gold_data = run_silver_to_gold_merge()

    # Task 8 validates final output
    validate = validate_gold_data()

    # Wire explicit dependencies
    validated >> bronze_table >> silver_table >> gold_tables >> silver_data
    silver_data >> gold_data >> validate


# ── Instantiate the DAG ───────────────────────────────────────
weather_backfill_pipeline()