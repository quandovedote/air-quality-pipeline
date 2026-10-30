-- Calculate daily PM2.5 averages and coverage from stored observations.
CREATE OR REPLACE VIEW daily_pm25_summary AS
SELECT
    station,
    observed_at::date AS observation_date,

    -- Count received rows separately from available PM2.5 values.
    COUNT(*) AS received_hours,
    COUNT(pm25) AS valid_pm25_hours,

    -- AVG ignores NULL values; all missing values produce a NULL average.
    ROUND(AVG(pm25), 3) AS available_readings_avg_pm25,

    -- Expect one hourly PM2.5 reading for each of the day's 24 hours.
    ROUND(COUNT(pm25) * 100.0 / 24, 1) AS coverage_percent,
    CASE
        WHEN COUNT(pm25) = 24 THEN 'complete'
        WHEN COUNT(pm25) = 0 THEN 'no_valid_measurements'
        ELSE 'incomplete'
    END AS coverage_status
FROM observations
GROUP BY station, observed_at::date;