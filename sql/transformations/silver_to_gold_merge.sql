-- ============================================================
-- SILVER → GOLD TRANSFORMATION
-- Purpose: Aggregate hourly Silver data into daily Gold summary
--          and detect extreme weather events
-- Type: Athena MERGE INTO (ACID upsert via Iceberg)
-- Triggered by: Airflow AthenaOperator
-- Input: weather_lakehouse.silver_weather_hourly
-- Output: weather_lakehouse.gold_daily_weather_summary
--         weather_lakehouse.gold_extreme_weather_events
-- Runs: Daily after Silver transformation completes
-- ============================================================


-- ============================================================
-- MERGE 1: Daily Weather Summary
-- One row per city per day
-- Updates if city+date exists, inserts if new
-- ============================================================

MERGE INTO weather_lakehouse.gold_daily_weather_summary AS target

USING (
    SELECT
        city_name,
        city_display_name,
        country,
        observation_date                            AS weather_date,

        -- Temperature aggregations
        ROUND(AVG(temperature_2m), 2)               AS avg_temperature,
        ROUND(MAX(temperature_2m), 2)               AS max_temperature,
        ROUND(MIN(temperature_2m), 2)               AS min_temperature,
        ROUND(AVG(apparent_temperature), 2)         AS avg_apparent_temperature,

        -- Humidity aggregations
        ROUND(AVG(relative_humidity_2m), 2)         AS avg_humidity,
        MAX(relative_humidity_2m)                   AS max_humidity,
        MIN(relative_humidity_2m)                   AS min_humidity,

        -- Precipitation aggregations
        ROUND(SUM(precipitation), 2)                AS total_precipitation,
        ROUND(SUM(rain), 2)                         AS total_rain,
        CASE
            WHEN SUM(precipitation) > 0
            THEN TRUE
            ELSE FALSE
        END                                         AS had_rain,

        -- Wind aggregations
        ROUND(AVG(wind_speed_10m), 2)               AS avg_wind_speed,
        ROUND(MAX(wind_speed_10m), 2)               AS max_wind_speed,

        -- Dominant wind direction (most frequent)
        APPROX_MOST_FREQUENT(wind_direction_10m, 1)[1]
                                                    AS dominant_wind_direction,

        -- Dominant weather condition (most frequent across 24 hours)
        MAX_BY(weather_condition, condition_count)  AS dominant_condition,
        MAX_BY(weather_code, condition_count)       AS dominant_weather_code,

        -- UV aggregations
        ROUND(MAX(uv_index), 2)                     AS max_uv_index,
        ROUND(AVG(uv_index), 2)                     AS avg_uv_index,

        -- Derived metrics
        ROUND(MAX(temperature_2m) - MIN(temperature_2m), 2)
                                                    AS temperature_range,

        -- Comfort score: ideal is 22°C and 50% humidity
        -- Penalise deviation from ideal
        GREATEST(0, LEAST(100,
            ROUND(
                100
                - (ABS(AVG(temperature_2m) - 22) * 2)
                - (ABS(AVG(relative_humidity_2m) - 50) * 0.3),
            2)
        ))                                          AS comfort_score,

        -- Pipeline metadata
        CURRENT_DATE                                AS last_updated_date,
        CAST('{{ run_date }}' AS DATE)              AS pipeline_run_date,
        COUNT(*)                                    AS record_count

    FROM (
        SELECT
            city_name,
            city_display_name,
            country,
            observation_date,
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
            -- Count frequency of each condition for dominant calculation
            COUNT(*) OVER (
                PARTITION BY city_name, observation_date, weather_condition
            )                                       AS condition_count
        FROM weather_lakehouse.silver_weather_hourly
        WHERE observation_date = DATE '{{ run_date }}'
        AND   data_quality_flag != 'BAD'
    ) daily_data
    GROUP BY
        city_name,
        city_display_name,
        country,
        observation_date
) AS source

ON  target.city_name    = source.city_name
AND target.weather_date = source.weather_date

-- City+date already exists in Gold → update all metrics
WHEN MATCHED THEN UPDATE SET
    avg_temperature         = source.avg_temperature,
    max_temperature         = source.max_temperature,
    min_temperature         = source.min_temperature,
    avg_apparent_temperature = source.avg_apparent_temperature,
    avg_humidity            = source.avg_humidity,
    max_humidity            = source.max_humidity,
    min_humidity            = source.min_humidity,
    total_precipitation     = source.total_precipitation,
    total_rain              = source.total_rain,
    had_rain                = source.had_rain,
    avg_wind_speed          = source.avg_wind_speed,
    max_wind_speed          = source.max_wind_speed,
    dominant_wind_direction = source.dominant_wind_direction,
    dominant_condition      = source.dominant_condition,
    dominant_weather_code   = source.dominant_weather_code,
    max_uv_index            = source.max_uv_index,
    avg_uv_index            = source.avg_uv_index,
    temperature_range       = source.temperature_range,
    comfort_score           = source.comfort_score,
    last_updated_date       = source.last_updated_date,
    pipeline_run_date       = source.pipeline_run_date,
    record_count            = source.record_count

-- New city+date → insert fresh row
WHEN NOT MATCHED THEN INSERT (
    city_name,
    city_display_name,
    country,
    weather_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    avg_apparent_temperature,
    avg_humidity,
    max_humidity,
    min_humidity,
    total_precipitation,
    total_rain,
    had_rain,
    avg_wind_speed,
    max_wind_speed,
    dominant_wind_direction,
    dominant_condition,
    dominant_weather_code,
    max_uv_index,
    avg_uv_index,
    temperature_range,
    comfort_score,
    first_seen_date,
    last_updated_date,
    pipeline_run_date,
    record_count
)
VALUES (
    source.city_name,
    source.city_display_name,
    source.country,
    source.weather_date,
    source.avg_temperature,
    source.max_temperature,
    source.min_temperature,
    source.avg_apparent_temperature,
    source.avg_humidity,
    source.max_humidity,
    source.min_humidity,
    source.total_precipitation,
    source.total_rain,
    source.had_rain,
    source.avg_wind_speed,
    source.max_wind_speed,
    source.dominant_wind_direction,
    source.dominant_condition,
    source.dominant_weather_code,
    source.max_uv_index,
    source.avg_uv_index,
    source.temperature_range,
    source.comfort_score,
    CURRENT_DATE,
    source.last_updated_date,
    source.pipeline_run_date,
    source.record_count
);


-- ============================================================
-- MERGE 2: Extreme Weather Events
-- Only inserts rows when thresholds are crossed
-- ============================================================

MERGE INTO weather_lakehouse.gold_extreme_weather_events AS target

USING (
    SELECT
        city_name,
        city_display_name,
        country,
        weather_date                                AS event_date,

        -- Classify event type based on thresholds
        CASE
            WHEN max_temperature > 42
                 THEN 'EXTREME_HEAT'
            WHEN min_temperature < 0
                 THEN 'EXTREME_COLD'
            WHEN total_precipitation > 50
                 THEN 'HEAVY_RAIN'
            WHEN max_wind_speed > 60
                 THEN 'STRONG_WIND'
            WHEN max_uv_index > 8
                 THEN 'HIGH_UV'
            WHEN dominant_weather_code IN (95, 96, 99)
                 THEN 'THUNDERSTORM'
            ELSE NULL
        END                                         AS event_type,

        -- Classify severity
        CASE
            WHEN max_temperature > 48
                 OR total_precipitation > 100
                 OR max_wind_speed > 120
                 THEN 'CRITICAL'
            WHEN max_temperature > 45
                 OR total_precipitation > 75
                 OR max_wind_speed > 90
                 THEN 'WARNING'
            ELSE 'WATCH'
        END                                         AS severity,

        max_temperature,
        min_temperature,
        total_precipitation,
        max_wind_speed,
        max_uv_index,
        dominant_condition,
        CURRENT_DATE                                AS detected_on,
        CAST('{{ run_date }}' AS DATE)              AS pipeline_run_date

    FROM weather_lakehouse.gold_daily_weather_summary
    WHERE weather_date = DATE '{{ run_date }}'

    -- Only rows that crossed at least one threshold
    AND (
        max_temperature     > 42
        OR min_temperature  < 0
        OR total_precipitation > 50
        OR max_wind_speed   > 60
        OR max_uv_index     > 8
        OR dominant_weather_code IN (95, 96, 99)
    )
) AS source

-- Only proceed if an event was classified
ON source.event_type IS NOT NULL

AND target.city_name  = source.city_name
AND target.event_date = source.event_date
AND target.event_type = source.event_type

-- Event already recorded → update severity if changed
WHEN MATCHED THEN UPDATE SET
    severity            = source.severity,
    max_temperature     = source.max_temperature,
    min_temperature     = source.min_temperature,
    total_precipitation = source.total_precipitation,
    max_wind_speed      = source.max_wind_speed,
    max_uv_index        = source.max_uv_index,
    dominant_condition  = source.dominant_condition,
    pipeline_run_date   = source.pipeline_run_date

-- New extreme event → insert it
WHEN NOT MATCHED AND source.event_type IS NOT NULL
THEN INSERT (
    city_name,
    city_display_name,
    country,
    event_date,
    event_type,
    severity,
    max_temperature,
    min_temperature,
    total_precipitation,
    max_wind_speed,
    max_uv_index,
    dominant_condition,
    detected_on,
    pipeline_run_date
)
VALUES (
    source.city_name,
    source.city_display_name,
    source.country,
    source.event_date,
    source.event_type,
    source.severity,
    source.max_temperature,
    source.min_temperature,
    source.total_precipitation,
    source.max_wind_speed,
    source.max_uv_index,
    source.dominant_condition,
    source.detected_on,
    source.pipeline_run_date
);