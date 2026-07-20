-- ============================================================
-- DASHBOARD INSIGHT 2: Rainiest Cities This Month
-- Purpose: Rank cities by total rainfall and rainy days
-- Real world use case: Agriculture planning, flood risk,
--                      travel advisories
-- Run: Directly in Athena console for dashboard
-- ============================================================

SELECT
    city_display_name,
    country,

    -- Total rainfall this month
    ROUND(SUM(total_precipitation), 2)          AS total_precipitation_mm,
    ROUND(SUM(total_rain), 2)                   AS total_rain_mm,

    -- How many days had rain
    COUNT(CASE WHEN had_rain = TRUE THEN 1 END) AS rainy_days,

    -- Total days in period
    COUNT(*)                                    AS total_days,

    -- Percentage of days with rain
    ROUND(
        COUNT(CASE WHEN had_rain = TRUE THEN 1 END) * 100.0 / COUNT(*),
    1)                                          AS rainy_day_percentage,

    -- Average rainfall on rainy days only
    ROUND(
        SUM(total_precipitation) /
        NULLIF(COUNT(CASE WHEN had_rain = TRUE THEN 1 END), 0),
    2)                                          AS avg_rain_per_rainy_day_mm,

    -- Heaviest single day rainfall
    MAX(total_precipitation)                    AS max_single_day_rain_mm,

    -- Rank by total precipitation
    RANK() OVER (
        ORDER BY SUM(total_precipitation) DESC
    )                                           AS rain_rank,

    -- Average comfort score this month
    ROUND(AVG(comfort_score), 1)                AS avg_comfort_score

FROM weather_lakehouse.gold_daily_weather_summary
WHERE weather_date >= DATE_TRUNC('month', CURRENT_DATE)
AND   weather_date <  CURRENT_DATE
GROUP BY
    city_name,
    city_display_name,
    country
ORDER BY
    total_precipitation_mm DESC;