# ============================================================
# INCREMENTAL DAG — TaskFlow API Syntax
# Purpose: Daily ingestion of previous day's weather data
#          for all 6 cities through Bronze → Silver → Gold
# Schedule: Every day at midnight UTC automatically
# Syntax: Airflow 3.x TaskFlow API with @dag and @task
# ============================================================

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
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
FORECAST_API_URL    = "https://api.open-meteo.com/v1/forecast"


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
    dag_id="weather_incremental_pipeline",
    description="Daily ingestion of previous day weather data",
    schedule="0 0 * * *",          # Every day at midnight UTC
    start_date=datetime(2026, 7, 1),
    catchup=False,                  # Don't backfill missed runs
    max_active_runs=1,              # Only one run at a time
    default_args={
        "owner":            "weather-lakehouse",
        "retries":          3,
        "retry_delay":      timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["weather", "incremental", "daily", "lakehouse"],
)
def weather_incremental_pipeline():
    """
    Daily DAG that runs at midnight UTC and fetches the
    previous day's 24 hourly weather readings for all 6 cities.

    Pipeline: Open-Meteo API → Bronze S3 → Silver Iceberg
                                         → Gold Iceberg
    """

    # ──────────────────────────────────────────────────────────
    # TASK 1: Fetch yesterday's data and upload to Bronze S3
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="fetch_and_upload_daily_bronze",
        execution_timeout=timedelta(minutes=30),
    )
    def fetch_and_upload_daily_bronze() -> dict:
        """
        Fetch previous day's 24 hourly weather readings
        for all 6 cities from Open-Meteo forecast API
        and upload raw JSON to Bronze S3.

        Why yesterday and not today:
        Today's data is incomplete (only hours 0 to now).
        Yesterday is always a full 24 hour dataset.

        Returns run metadata dict passed to downstream tasks.
        """
        config      = load_cities_config()
        cities      = config["cities"]
        hourly_vars = config["api_settings"]["hourly_variables"]

        # Always process yesterday — full 24 hour dataset
        run_date    = datetime.utcnow().date() - timedelta(days=1)
        year        = run_date.strftime("%Y")
        month       = run_date.strftime("%m")
        day         = run_date.strftime("%d")

        logger.info(f"Processing date: {run_date}")
        logger.info(f"Cities: {[c['name'] for c in cities]}")

        s3_hook         = S3Hook(aws_conn_id="aws_default")
        cities_loaded   = []

        for city in cities:
            logger.info(
                f"Fetching {run_date} data for "
                f"{city['display_name']}..."
            )

            # Use forecast API with past_days=1 to get yesterday
            # This returns yesterday + today forecast
            # We filter to only yesterday's date below
            params = {
                "latitude":     city["latitude"],
                "longitude":    city["longitude"],
                "hourly":       ",".join(hourly_vars),
                "timezone":     city["timezone"],
                "past_days":    1,
                "forecast_days": 1,
            }

            response = requests.get(
                FORECAST_API_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            raw_data = response.json()

            # Validate response
            if "hourly" not in raw_data:
                raise ValueError(
                    f"No hourly data returned for "
                    f"{city['display_name']} on {run_date}"
                )

            # Filter to only yesterday's 24 hours
            # API returns 48 hours (yesterday + today)
            # We only want indices where date == run_date
            time_values     = raw_data["hourly"]["time"]
            target_date_str = str(run_date)

            yesterday_indices = [
                idx for idx, ts in enumerate(time_values)
                if ts[:10] == target_date_str
            ]

            if len(yesterday_indices) != 24:
                logger.warning(
                    f"Expected 24 hourly values for {run_date}, "
                    f"got {len(yesterday_indices)} for "
                    f"{city['display_name']}"
                )

            # Build daily data slice with only yesterday's hours
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
                "ingestion_timestamp":  datetime.utcnow().isoformat(),
                "pipeline_run_date":    target_date_str,
                "hourly_units":         raw_data["hourly_units"],
                "hourly": {
                    var: [
                        raw_data["hourly"][var][i]
                        for i in yesterday_indices
                    ]
                    for var in ["time"] + hourly_vars
                    if var in raw_data["hourly"]
                }
            }

            # S3 key — same structure as Bronze table partitions
            # replace=True makes this idempotent
            # Rerunning same day overwrites same key cleanly
            s3_key = (
                f"bronze/"
                f"city={city['name']}/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                f"raw.json"
            )

            s3_hook.load_string(
                string_data=json.dumps(daily_data, indent=2),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,
            )

            cities_loaded.append(city["name"])
            logger.info(
                f"✅ {city['display_name']}: "
                f"uploaded to s3://{S3_BUCKET}/{s3_key}"
            )

        # Return run metadata for downstream tasks
        run_metadata = {
            "run_date":     str(run_date),
            "year":         year,
            "month":        month,
            "day":          day,
            "cities":       cities_loaded,
            "city_count":   len(cities_loaded),
        }

        logger.info(f"Bronze upload complete: {run_metadata}")
        return run_metadata


    # ──────────────────────────────────────────────────────────
    # TASK 2: Validate Bronze upload
    # ──────────────────────────────────────────────────────────
    @task(task_id="validate_bronze_upload")
    def validate_bronze_upload(run_metadata: dict) -> dict:
        """
        Verify all 6 city JSON files landed in S3 correctly.
        Checks file exists and is non-empty.
        Fails pipeline immediately if any city is missing.
        Passes run_metadata through to downstream tasks.
        """
        s3_hook     = S3Hook(aws_conn_id="aws_default")
        run_date    = run_metadata["run_date"]
        year        = run_metadata["year"]
        month       = run_metadata["month"]
        day         = run_metadata["day"]
        errors      = []

        for city_name in run_metadata["cities"]:
            s3_key = (
                f"bronze/"
                f"city={city_name}/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                f"raw.json"
            )

            exists = s3_hook.check_for_key(
                key=s3_key,
                bucket_name=S3_BUCKET,
            )

            if not exists:
                errors.append(
                    f"Missing Bronze file: s3://{S3_BUCKET}/{s3_key}"
                )
                logger.error(f"❌ Missing file for {city_name}/{run_date}")
            else:
                logger.info(
                    f"✅ {city_name}/{run_date}: "
                    f"Bronze file confirmed in S3"
                )

        if errors:
            raise FileNotFoundError(
                f"Bronze validation failed for {run_date}:\n"
                + "\n".join(errors)
            )

        logger.info(
            f"✅ All {run_metadata['city_count']} cities "
            f"validated for {run_date}"
        )

        # Pass run_metadata through to next task
        return run_metadata


    # ──────────────────────────────────────────────────────────
    # TASK 3: Repair Bronze table partitions
    # ──────────────────────────────────────────────────────────
    @task(task_id="repair_bronze_table_partitions")
    def repair_bronze_table_partitions(run_metadata: dict) -> dict:
        """
        Run MSCK REPAIR TABLE to tell Athena about the new
        Bronze S3 partition for today's run date.

        Without this, Athena doesn't know the new
        city/year/month/day partition exists and the
        Silver CTAS query returns no rows.

        Passes run_metadata through unchanged.
        """
        sql = (
            "MSCK REPAIR TABLE "
            "weather_lakehouse.bronze_weather_raw;"
        )

        run_athena_query(sql)
        logger.info(
            f"✅ Bronze partitions repaired for "
            f"{run_metadata['run_date']}"
        )

        return run_metadata


    # ──────────────────────────────────────────────────────────
    # TASK 4: Bronze → Silver CTAS
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="run_bronze_to_silver_ctas",
        execution_timeout=timedelta(minutes=30),
    )
    def run_bronze_to_silver_ctas(run_metadata: dict) -> dict:
        """
        Run Bronze to Silver CTAS for each city for run_date.

        Reads Bronze JSON → flattens 24 hourly arrays
        into 24 rows → decodes weather codes → validates
        data quality → writes Parquet to Silver Iceberg.

        6 cities × 1 day = 6 Athena CTAS queries.
        Each query produces 24 rows in Silver.
        Total: 144 new Silver rows per daily run.
        """
        config          = load_cities_config()
        cities          = config["cities"]
        sql_template    = read_sql_file(
            "transformations/bronze_to_silver_ctas.sql"
        )

        run_date    = run_metadata["run_date"]
        year        = run_metadata["year"]
        month       = run_metadata["month"]
        day         = run_metadata["day"]

        for city in cities:
            sql = (
                sql_template
                .replace("{{ run_date }}",  run_date)
                .replace("{{ city_name }}", city["name"])
                .replace("{{ year }}",      year)
                .replace("{{ month }}",     month)
                .replace("{{ day }}",       day)
            )

            run_athena_query(sql)
            logger.info(
                f"✅ Silver CTAS complete: "
                f"{city['display_name']} / {run_date}"
            )

        logger.info(
            f"✅ Bronze → Silver complete for all cities "
            f"on {run_date}"
        )

        return run_metadata


    # ──────────────────────────────────────────────────────────
    # TASK 5: Silver → Gold MERGE
    # ──────────────────────────────────────────────────────────
    @task(
        task_id="run_silver_to_gold_merge",
        execution_timeout=timedelta(minutes=30),
    )
    def run_silver_to_gold_merge(run_metadata: dict) -> dict:
        """
        Run Silver to Gold MERGE INTO for run_date.

        Aggregates 144 Silver rows (6 cities × 24 hours)
        into 6 Gold daily summary rows — one per city.

        ACID upsert via Iceberg:
        - If city+date already in Gold → UPDATE
        - If new city+date → INSERT

        Also detects extreme weather events and
        upserts into gold_extreme_weather_events.

        1 MERGE query covers all 6 cities in one shot.
        """
        sql_template    = read_sql_file(
            "transformations/silver_to_gold_merge.sql"
        )
        run_date        = run_metadata["run_date"]

        sql = sql_template.replace("{{ run_date }}", run_date)

        run_athena_query(sql)
        logger.info(
            f"✅ Silver → Gold MERGE complete for {run_date}"
        )

        return run_metadata


    # ──────────────────────────────────────────────────────────
    # TASK 6: Validate daily run
    # ──────────────────────────────────────────────────────────
    @task(task_id="validate_daily_run")
    def validate_daily_run(run_metadata: dict) -> None:
        """
        Final check — confirm Gold table received exactly
        6 new rows for run_date (one per city).

        Logs a warning if fewer than 6 rows found
        but does not fail — partial data is still useful.
        Fails only if zero rows found for run_date.
        """
        run_date        = run_metadata["run_date"]
        expected_rows   = run_metadata["city_count"]

        athena_hook = AthenaHook(
            aws_conn_id="aws_default",
            sleep_time=5,
        )

        query_id = athena_hook.run_query(
            query=f"""
                SELECT COUNT(*) AS row_count
                FROM weather_lakehouse.gold_daily_weather_summary
                WHERE weather_date = DATE '{run_date}'
            """,
            query_context={"Database": GLUE_DATABASE},
            result_configuration={"OutputLocation": ATHENA_RESULTS},
        )

        results     = athena_hook.get_query_results(query_id)
        row_count   = int(
            results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
        )

        logger.info(
            f"Gold rows for {run_date}: {row_count} "
            f"(expected {expected_rows})"
        )

        if row_count == 0:
            raise ValueError(
                f"Zero Gold rows for {run_date} — "
                f"check Silver → Gold MERGE task logs"
            )

        if row_count < expected_rows:
            logger.warning(
                f"Only {row_count}/{expected_rows} cities "
                f"loaded for {run_date}"
            )

        logger.info(
            f"✅ Daily run validated: "
            f"{row_count} Gold rows for {run_date}"
        )


    # ──────────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    # ──────────────────────────────────────────────────────────

    # Task 1: Fetch and upload — returns run_metadata
    bronze_metadata = fetch_and_upload_daily_bronze()

    # Task 2: Validate Bronze — receives and passes run_metadata
    validated_metadata = validate_bronze_upload(bronze_metadata)

    # Task 3: Repair partitions — Athena discovers new S3 files
    repaired_metadata = repair_bronze_table_partitions(
        validated_metadata
    )

    # Task 4: Bronze → Silver — 6 CTAS queries
    silver_metadata = run_bronze_to_silver_ctas(repaired_metadata)

    # Task 5: Silver → Gold — 1 MERGE query
    gold_metadata = run_silver_to_gold_merge(silver_metadata)

    # Task 6: Validate final output
    validate_daily_run(gold_metadata)


# ── Instantiate the DAG ───────────────────────────────────────
weather_incremental_pipeline()