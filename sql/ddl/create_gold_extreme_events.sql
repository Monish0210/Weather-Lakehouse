-- ============================================================
-- GOLD LAYER: Iceberg Table Creation
-- Purpose: Analysis-ready aggregated weather data
-- Type: Apache Iceberg tables in S3 Tables
-- Format: Parquet columnar storage
-- Run: Once manually in Athena before first pipeline run
-- ============================================================

-- ============================================================
-- GOLD TABLE 2: Extreme Weather Events
-- Only rows where weather crossed dangerous thresholds
-- Powers the "extreme weather" dashboard panel
-- New rows inserted when thresholds are crossed
-- Existing rows updated if same city+date reprocessed
-- ============================================================

CREATE TABLE IF NOT EXISTS
weather_lakehouse.gold_extreme_weather_events (

    -- Identity
    city_name               VARCHAR,
    city_display_name       VARCHAR,
    country                 VARCHAR,
    event_date              DATE,

    -- What made it extreme
    event_type              VARCHAR,
    severity                VARCHAR,

    -- The actual values that triggered the alert
    max_temperature         DOUBLE,
    min_temperature         DOUBLE,
    total_precipitation     DOUBLE,
    max_wind_speed          DOUBLE,
    max_uv_index            DOUBLE,
    dominant_condition      VARCHAR,

    -- Pipeline metadata
    detected_on             DATE,
    pipeline_run_date       DATE

)
LOCATION 's3tablescatalog/weather-lakehouse-tables-monish/weather_lakehouse/gold_extreme_weather_events'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'PARQUET',
    'write_compression' = 'SNAPPY'
);