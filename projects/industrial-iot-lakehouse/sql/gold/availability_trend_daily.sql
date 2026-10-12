-- Fleet availability and failures per day (trend).
-- Grain of the result: one row per UTC day of the observation period.
-- Ratios from summed seconds (no average of daily per-equipment ratios).
-- Joins: none (agg_equipment_daily already has one row per equipment and day).
WITH daily_totals AS (
    SELECT
        date,
        COUNT(*) AS equipment,
        SUM(scheduled_seconds) AS scheduled_seconds,
        SUM(operating_seconds) AS operating_seconds,
        SUM(unplanned_seconds) AS unplanned_seconds,
        SUM(failures) AS failures
    FROM agg_equipment_daily
    GROUP BY date
)

SELECT
    date,
    equipment,
    failures,
    ROUND(unplanned_seconds / 3600.0, 2) AS unplanned_downtime_hours,
    ROUND(operating_seconds / NULLIF(scheduled_seconds, 0), 4) AS availability
FROM daily_totals
ORDER BY date;
