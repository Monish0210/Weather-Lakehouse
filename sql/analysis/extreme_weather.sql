-- ============================================================
-- DASHBOARD INSIGHT 3: Extreme Weather Event Analysis
-- Purpose: Show frequency and severity of extreme weather
--          events across all cities
-- Real world use case: Insurance risk scoring,
--                      emergency preparedness,
--                      travel safety advisories
-- Run: Directly in Athena console for dashboard
-- ============================================================

-- Part A: Extreme event summary by city
SELECT
    'CITY_SUMMARY'                              AS report_section,
    e.city_display_name,
    e.country,

    -- Total extreme events
    COUNT(*)                                    AS total_extreme_events,

    -- Events by type
    COUNT(CASE WHEN e.event_type = 'EXTREME_HEAT'   THEN 1 END)
                                                AS heat_events,
    COUNT(CASE WHEN e.event_type = 'EXTREME_COLD'   THEN 1 END)
                                                AS cold_events,
    COUNT(CASE WHEN e.event_type = 'HEAVY_RAIN'     THEN 1 END)
                                                AS heavy_rain_events,
    COUNT(CASE WHEN e.event_type = 'STRONG_WIND'    THEN 1 END)
                                                AS strong_wind_events,
    COUNT(CASE WHEN e.event_type = 'HIGH_UV'        THEN 1 END)
                                                AS high_uv_events,
    COUNT(CASE WHEN e.event_type = 'THUNDERSTORM'   THEN 1 END)
                                                AS thunderstorm_events,

    -- Events by severity
    COUNT(CASE WHEN e.severity = 'CRITICAL'     THEN 1 END)
                                                AS critical_events,
    COUNT(CASE WHEN e.severity = 'WARNING'      THEN 1 END)
                                                AS warning_events,
    COUNT(CASE WHEN e.severity = 'WATCH'        THEN 1 END)
                                                AS watch_events,

    -- Most recent extreme event
    MAX(e.event_date)                           AS most_recent_event,

    -- Most common event type for this city
    MAX_BY(e.event_type, event_count)           AS most_common_event_type,

    -- Risk score: weighted sum of events by severity
    SUM(
        CASE e.severity
            WHEN 'CRITICAL' THEN 3
            WHEN 'WARNING'  THEN 2
            WHEN 'WATCH'    THEN 1
            ELSE 0
        END
    )                                           AS risk_score

FROM weather_lakehouse.gold_extreme_weather_events e
JOIN (
    SELECT
        city_name,
        event_type,
        COUNT(*) AS event_count
    FROM weather_lakehouse.gold_extreme_weather_events
    GROUP BY city_name, event_type
) event_counts
ON  e.city_name  = event_counts.city_name
AND e.event_type = event_counts.event_type
WHERE e.event_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY
    e.city_name,
    e.city_display_name,
    e.country
ORDER BY
    risk_score DESC;


-- ============================================================
-- Part B: Recent extreme events timeline
-- Last 30 days — detailed event log
-- ============================================================

SELECT
    'EVENT_TIMELINE'                            AS report_section,
    event_date,
    city_display_name,
    country,
    event_type,
    severity,
    max_temperature,
    min_temperature,
    total_precipitation,
    max_wind_speed,
    max_uv_index,
    dominant_condition

FROM weather_lakehouse.gold_extreme_weather_events
WHERE event_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY
    event_date      DESC,
    severity        DESC;