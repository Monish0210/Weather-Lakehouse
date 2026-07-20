-- ============================================================
-- SILVER LAYER: Iceberg Table Creation
-- Purpose: Store cleaned, flattened, typed weather data
-- Type: Apache Iceberg table in S3 Tables
-- Format: Parquet columnar storage
-- Run: Once manually in Athena before first pipeline run
-- ============================================================

CREATE TABLE IF NOT EXISTS
weather_lakehouse.silver_weather_hourly (

    -- Location identifiers
    city_name               VARCHAR,
    city_display_name       VARCHAR,
    country                 VARCHAR,
    latitude                DOUBLE,
    longitude               DOUBLE,

    -- Time
    observation_timestamp   TIMESTAMP,
    observation_date        DATE,
    observation_hour        INT,
    timezone                VARCHAR,

    -- Temperature metrics
    temperature_2m          DOUBLE,
    apparent_temperature    DOUBLE,

    -- Humidity
    relative_humidity_2m    INT,

    -- Precipitation
    precipitation           DOUBLE,
    rain                    DOUBLE,

    -- Wind
    wind_speed_10m          DOUBLE,
    wind_direction_10m      INT,

    -- Weather condition
    weather_code            INT,
    weather_condition       VARCHAR,

    -- UV
    uv_index                DOUBLE,

    -- Pipeline metadata
    ingestion_timestamp     TIMESTAMP,
    pipeline_run_date       DATE,
    data_quality_flag       VARCHAR

)
PARTITIONED BY (observation_date)
LOCATION 's3tablescatalog/weather-lakehouse-tables-monish/weather_lakehouse/silver_weather_hourly'
TBLPROPERTIES (
    'table_type'            = 'ICEBERG',
    'format'                = 'PARQUET',
    'write_compression'     = 'SNAPPY',
    'optimize_rewrite_delete_file_threshold' = '10'
);