-- Update the statistics PostgreSQL uses to choose query plans.
ANALYZE observations;
ANALYZE stations;

-- Analyst retrieves one station's observations for one day.
EXPLAIN (ANALYZE, BUFFERS)
SELECT observed_at, pm25
FROM observations
WHERE station = 'Aotizhongxin'
  AND observed_at >= TIMESTAMP '2013-03-01 00:00:00'
  AND observed_at < TIMESTAMP '2013-03-02 00:00:00'
ORDER BY observed_at;

-- Analyst requests daily reports across stations.
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM report_daily_pm25(
    DATE '2013-03-01',
    DATE '2013-03-05'
)
ORDER BY observation_date, station;