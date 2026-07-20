-- ============================================================
-- DASHBOARD INSIGHT 1: Temperature Trend by City
-- Purpose: Show how temperature changed over last 30 days
--          across all 6 cities
-- Real world use case: Urban heat analysis, climate monitoring
-- Run: Directly in Athena console for dashboard
-- ============================================================

SELECT
    city_display_name,
    country,
    weather_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    temperature_range,
    comfort_score,
    dominant_condition,

    -- 7 day rolling average temperature per city
    ROUND(
        AVG(avg_temperature) OVER (
            PARTITION BY city_name
            ORDER BY weather_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
    2)                                          AS rolling_7day_avg_temp,

    -- Day over day temperature change
    ROUND(
        avg_temperature - LAG(avg_temperature, 1) OVER (
            PARTITION BY city_name
            ORDER BY weather_date
        ),
    2)                                          AS temp_change_from_yesterday,

    -- Rank cities by temperature on each day
    -- 1 = hottest city that day
    RANK() OVER (
        PARTITION BY weather_date
        ORDER BY avg_temperature DESC
    )                                           AS heat_rank_that_day

FROM weather_lakehouse.gold_daily_weather_summary
WHERE weather_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY
    city_name   ASC,
    weather_date ASC;