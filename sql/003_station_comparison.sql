-- Compare only station-days with complete PM2.5 coverage.
SELECT
    station,
    observation_date,
    available_readings_avg_pm25 AS daily_avg_pm25,
    valid_pm25_hours,
    coverage_percent
FROM report_daily_pm25(
    DATE '2013-03-04',
    DATE '2013-03-05'
)
WHERE coverage_status = 'complete'
ORDER BY observation_date, daily_avg_pm25 DESC, station;

-- Show all excluded station-days and their coverage.
SELECT
    station,
    observation_date,
    available_readings_avg_pm25,
    received_hours,
    valid_pm25_hours,
    coverage_percent,
    coverage_status
FROM report_daily_pm25(
    DATE '2013-03-04',
    DATE '2013-03-05'
)
WHERE coverage_status <> 'complete'
ORDER BY observation_date, station;