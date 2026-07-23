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