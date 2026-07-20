-- ============================================================
-- GOLD LAYER: Iceberg Table Creation
-- Purpose: Analysis-ready aggregated weather data
-- Type: Apache Iceberg tables in S3 Tables
-- Format: Parquet columnar storage
-- Run: Once manually in Athena before first pipeline run
-- ============================================================


-- ============================================================
-- GOLD TABLE 1: Daily Weather Summary
-- One row per city per day
-- Aggregated from 24 hourly Silver readings
-- This is your main dashboard table
-- ============================================================

CREATE TABLE IF NOT EXISTS
weather_lakehouse.gold_daily_weather_summary (

    -- Identity
    city_name               VARCHAR,
    city_display_name       VARCHAR,
    country                 VARCHAR,
    weather_date            DATE,

    -- Temperature aggregations
    avg_temperature         DOUBLE,
    max_temperature         DOUBLE,
    min_temperature         DOUBLE,
    avg_apparent_temperature DOUBLE,

    -- Humidity
    avg_humidity            DOUBLE,
    max_humidity            INT,
    min_humidity            INT,

    -- Precipitation
    total_precipitation     DOUBLE,
    total_rain              DOUBLE,
    had_rain                BOOLEAN,

    -- Wind
    avg_wind_speed          DOUBLE,
    max_wind_speed          DOUBLE,
    dominant_wind_direction INT,

    -- Weather condition
    dominant_condition      VARCHAR,
    dominant_weather_code   INT,

    -- UV
    max_uv_index            DOUBLE,
    avg_uv_index            DOUBLE,

    -- Derived metrics (calculated during MERGE)
    temperature_range       DOUBLE,
    comfort_score           DOUBLE,

    -- Pipeline metadata
    first_seen_date         DATE,
    last_updated_date       DATE,
    pipeline_run_date       DATE,
    record_count            INT

)
LOCATION 's3tablescatalog/weather-lakehouse-tables-monish/weather_lakehouse/gold_daily_weather_summary'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'PARQUET',
    'write_compression' = 'SNAPPY'
);


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