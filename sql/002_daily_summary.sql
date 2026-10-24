CREATE OR REPLACE VIEW daily_pm25_summary AS
SELECT
    station,
    observed_at::date AS observation_date,
    COUNT(*) AS received_hours,
    COUNT(pm25) AS valid_pm25_hours,
    ROUND(AVG(pm25), 3) AS available_readings_avg_pm25,
    ROUND(COUNT(pm25) * 100.0 / 24, 1) AS coverage_percent,
    CASE
        WHEN COUNT(pm25) = 24 THEN 'complete'
        WHEN COUNT(pm25) = 0 THEN 'no_valid_measurements'
        ELSE 'incomplete'
    END AS coverage_status
FROM observations
GROUP BY station, observed_at::date;