-- Reading quality trend per equipment type and day: quarantined, physically impossible, late and
-- out-of-order readings, and their share of the readings received.
-- Grain of the result: one row per (date, equipment_type).
-- Joins: agg_equipment_daily N:1 dim_equipment (equipment_id).
WITH daily_with_type AS (
    SELECT
        d.date,
        COALESCE(e.equipment_type, 'unknown') AS equipment_type,
        d.readings,
        d.quarantined_readings,
        d.impossible_values,
        d.late_readings,
        d.out_of_order_readings
    FROM agg_equipment_daily AS d
    INNER JOIN dim_equipment AS e
        ON d.equipment_id = e.equipment_id  -- N:1
),

type_totals AS (
    SELECT
        date,
        equipment_type,
        SUM(readings) AS readings,
        SUM(quarantined_readings) AS quarantined_readings,
        SUM(impossible_values) AS impossible_values,
        SUM(late_readings) AS late_readings,
        SUM(out_of_order_readings) AS out_of_order_readings
    FROM daily_with_type
    GROUP BY date, equipment_type
)

SELECT
    date,
    equipment_type,
    readings,
    quarantined_readings,
    impossible_values,
    late_readings,
    out_of_order_readings,
    ROUND(quarantined_readings / NULLIF(readings + quarantined_readings, 0), 4) AS quarantine_rate
FROM type_totals
ORDER BY date, equipment_type;
