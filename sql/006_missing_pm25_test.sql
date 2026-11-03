-- Start a transaction so the test observations can be removed afterwards.
BEGIN;

-- Add a hypothetical future day for an existing station.
INSERT INTO observations (station, observed_at, pm25)
SELECT
    'Aotizhongxin',
    hour_value,
    NULL
FROM generate_series(
    TIMESTAMP '2099-01-01 00:00:00',
    TIMESTAMP '2099-01-01 23:00:00',
    INTERVAL '1 hour'
) AS hours(hour_value);

-- Inspect the reporting behaviour.
SELECT *
FROM report_daily_pm25(
    DATE '2099-01-01',
    DATE '2099-01-01'
)
WHERE station = 'Aotizhongxin';

-- Automatically check the expected result.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM report_daily_pm25(
            DATE '2099-01-01',
            DATE '2099-01-01'
        )
        WHERE station = 'Aotizhongxin'
          AND received_hours = 24
          AND valid_pm25_hours = 0
          AND available_readings_avg_pm25 IS NULL
          AND coverage_percent = 0
          AND coverage_status = 'no_valid_measurements'
    ) THEN
        RAISE EXCEPTION 'Missing-PM2.5 reporting test failed';
    END IF;

    RAISE NOTICE 'PASS: received records with no valid PM2.5';
END;
$$;

-- Undo the test inserts, leaving the stored observations unchanged.
ROLLBACK;