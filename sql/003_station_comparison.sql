-- Default comparison: include only days with complete PM2.5 coverage.
SELECT
    station,
    observation_date,
    available_readings_avg_pm25 AS daily_avg_pm25,
    valid_pm25_hours,
    coverage_percent
FROM daily_pm25_summary
WHERE observation_date = DATE '2013-03-04'
  AND coverage_status = 'complete'
ORDER BY daily_avg_pm25 DESC, station;

-- Make excluded results visible for investigation.
SELECT
    station,
    observation_date,
    available_readings_avg_pm25,
    valid_pm25_hours,
    coverage_percent,
    coverage_status
FROM daily_pm25_summary
WHERE observation_date = DATE '2013-03-04'
  AND coverage_status <> 'complete'
ORDER BY station;