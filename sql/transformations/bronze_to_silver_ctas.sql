-- ============================================================
-- BRONZE → SILVER TRANSFORMATION
-- Purpose: Flatten nested JSON arrays into clean tabular rows
-- Type: Athena CTAS (Create Table As Select)
-- Triggered by: Airflow AthenaOperator
-- Input: weather_lakehouse.bronze_weather_raw
-- Output: weather_lakehouse.silver_weather_hourly
-- Runs: Daily after Bronze upload completes
-- ============================================================

-- Step 1: Drop existing partition for this run date
-- This ensures idempotency — safe to rerun same day
ALTER TABLE weather_lakehouse.silver_weather_hourly
DROP PARTITION (observation_date = DATE '{{ run_date }}')
IF EXISTS;

-- Step 2: Insert flattened hourly rows into Silver
INSERT INTO weather_lakehouse.silver_weather_hourly

WITH

-- Unnest the time array with row index
-- This gives us each hour as a separate row
time_series AS (
    SELECT
        city,
        city_display_name,
        country,
        latitude,
        longitude,
        timezone,
        ingestion_timestamp,
        pipeline_run_date,
        hour_index,
        hourly.time[hour_index]                 AS raw_timestamp,
        hourly.temperature_2m[hour_index]       AS temperature_2m,
        hourly.relative_humidity_2m[hour_index] AS relative_humidity_2m,
        hourly.apparent_temperature[hour_index] AS apparent_temperature,
        hourly.precipitation[hour_index]        AS precipitation,
        hourly.rain[hour_index]                 AS rain,
        hourly.wind_speed_10m[hour_index]       AS wind_speed_10m,
        hourly.wind_direction_10m[hour_index]   AS wind_direction_10m,
        hourly.weather_code[hour_index]         AS weather_code,
        hourly.uv_index[hour_index]             AS uv_index
    FROM weather_lakehouse.bronze_weather_raw
    CROSS JOIN UNNEST(SEQUENCE(1, 24)) AS t(hour_index)
    WHERE city     = '{{ city_name }}'
    AND   year     = '{{ year }}'
    AND   month    = '{{ month }}'
    AND   day      = '{{ day }}'
),

-- Decode weather code to human readable condition
-- and apply data quality checks
enriched AS (
    SELECT
        city_name                                   AS city_name,
        city_display_name                           AS city_display_name,
        country                                     AS country,
        latitude                                    AS latitude,
        longitude                                   AS longitude,

        -- Parse timestamp string to proper TIMESTAMP type
        CAST(raw_timestamp AS TIMESTAMP)            AS observation_timestamp,
        CAST(CAST(raw_timestamp AS TIMESTAMP)
             AS DATE)                               AS observation_date,
        HOUR(CAST(raw_timestamp AS TIMESTAMP))      AS observation_hour,
        timezone                                    AS timezone,

        -- Temperature
        temperature_2m                              AS temperature_2m,
        apparent_temperature                        AS apparent_temperature,

        -- Humidity
        relative_humidity_2m                        AS relative_humidity_2m,

        -- Precipitation
        precipitation                               AS precipitation,
        rain                                        AS rain,

        -- Wind
        wind_speed_10m                              AS wind_speed_10m,
        wind_direction_10m                          AS wind_direction_10m,

        -- Weather code kept raw
        weather_code                                AS weather_code,

        -- Decode weather code to readable label
        CASE
            WHEN weather_code = 0               THEN 'Clear sky'
            WHEN weather_code = 1               THEN 'Mainly clear'
            WHEN weather_code = 2               THEN 'Partly cloudy'
            WHEN weather_code = 3               THEN 'Overcast'
            WHEN weather_code IN (45, 48)       THEN 'Fog'
            WHEN weather_code IN (51, 53, 55)   THEN 'Drizzle'
            WHEN weather_code IN (61, 63, 65)   THEN 'Rain'
            WHEN weather_code IN (71, 73, 75)   THEN 'Snow'
            WHEN weather_code IN (77)           THEN 'Snow grains'
            WHEN weather_code IN (80, 81, 82)   THEN 'Rain showers'
            WHEN weather_code IN (85, 86)       THEN 'Snow showers'
            WHEN weather_code = 95              THEN 'Thunderstorm'
            WHEN weather_code IN (96, 99)       THEN 'Thunderstorm with hail'
            ELSE                                     'Unknown'
        END                                         AS weather_condition,

        -- UV index
        uv_index                                    AS uv_index,

        -- Pipeline metadata
        CAST(ingestion_timestamp AS TIMESTAMP)      AS ingestion_timestamp,
        CAST(pipeline_run_date AS DATE)             AS pipeline_run_date,

        -- Data quality flag
        CASE
            WHEN temperature_2m IS NULL
                 OR relative_humidity_2m IS NULL
                 OR wind_speed_10m IS NULL
                 THEN 'BAD'
            WHEN temperature_2m < -60
                 OR temperature_2m > 60
                 THEN 'SUSPECT'
            WHEN relative_humidity_2m < 0
                 OR relative_humidity_2m > 100
                 THEN 'SUSPECT'
            WHEN wind_speed_10m < 0
                 OR wind_speed_10m > 300
                 THEN 'SUSPECT'
            ELSE 'GOOD'
        END                                         AS data_quality_flag

    FROM time_series
)

-- Final select — only insert GOOD and SUSPECT rows
-- BAD rows are skipped to keep Silver clean
SELECT
    city_name,
    city_display_name,
    country,
    latitude,
    longitude,
    observation_timestamp,
    observation_date,
    observation_hour,
    timezone,
    temperature_2m,
    apparent_temperature,
    relative_humidity_2m,
    precipitation,
    rain,
    wind_speed_10m,
    wind_direction_10m,
    weather_code,
    weather_condition,
    uv_index,
    ingestion_timestamp,
    pipeline_run_date,
    data_quality_flag
FROM enriched
WHERE data_quality_flag != 'BAD';