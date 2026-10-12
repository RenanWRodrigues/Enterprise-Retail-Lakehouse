-- Reliability per equipment type over the observation period (docs/GOLD.md, "Fórmulas").
-- Grain of the result: one row per equipment_type.
-- Ratios are recomputed from summed seconds and counts, never averaged from per-equipment ratios.
-- Joins: agg_equipment_reliability N:1 dim_equipment (equipment_id).
WITH reliability_with_type AS (
    SELECT
        COALESCE(e.equipment_type, 'unknown') AS equipment_type,
        r.equipment_id,
        r.scheduled_seconds,
        r.operating_seconds,
        r.unplanned_seconds,
        r.planned_seconds,
        r.failures,
        r.repairs,
        r.repair_seconds
    FROM agg_equipment_reliability AS r
    INNER JOIN dim_equipment AS e
        ON r.equipment_id = e.equipment_id  -- N:1 (the gate guarantees every key exists)
),

type_totals AS (
    SELECT
        equipment_type,
        COUNT(DISTINCT equipment_id) AS equipment,
        SUM(scheduled_seconds) AS scheduled_seconds,
        SUM(operating_seconds) AS operating_seconds,
        SUM(unplanned_seconds) AS unplanned_seconds,
        SUM(planned_seconds) AS planned_seconds,
        SUM(failures) AS failures,
        SUM(repairs) AS repairs,
        SUM(repair_seconds) AS repair_seconds
    FROM reliability_with_type
    GROUP BY equipment_type
)

SELECT
    equipment_type,
    equipment,
    failures,
    ROUND(unplanned_seconds / 3600.0, 2) AS unplanned_downtime_hours,
    ROUND(planned_seconds / 3600.0, 2) AS planned_downtime_hours,
    ROUND(operating_seconds / NULLIF(scheduled_seconds, 0), 4) AS availability,
    ROUND(operating_seconds / NULLIF(failures, 0) / 3600.0, 2) AS mtbf_hours,
    ROUND(repair_seconds / NULLIF(repairs, 0) / 3600.0, 2) AS mttr_hours
FROM type_totals
ORDER BY equipment_type;
