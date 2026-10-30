-- Keep a list of expected stations, even when they send no observations.
CREATE TABLE IF NOT EXISTS stations (
    station TEXT PRIMARY KEY
);

INSERT INTO stations (station)
VALUES
    ('Aotizhongxin'),
    ('Changping'),
    ('Dingling'),
    ('Dongsi'),
    ('Guanyuan'),
    ('Gucheng'),
    ('Huairou'),
    ('Nongzhanguan'),
    ('Shunyi'),
    ('Tiantan'),
    ('Wanliu'),
    ('Wanshouxigong')
-- Skip names that are already registered.
ON CONFLICT DO NOTHING;


-- Return daily results for every station between the two dates, inclusive.
CREATE OR REPLACE FUNCTION report_daily_pm25(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE (
    station TEXT,
    observation_date DATE,
    received_hours BIGINT,
    valid_pm25_hours BIGINT,
    available_readings_avg_pm25 NUMERIC,
    coverage_percent NUMERIC,
    coverage_status TEXT
)
LANGUAGE SQL
STABLE
AS $$
    -- Create one row for each requested day.
    WITH requested_dates AS (
        SELECT day_value::date AS observation_date
        FROM generate_series(
            start_date::timestamp,
            end_date::timestamp,
            INTERVAL '1 day'
        ) AS dates(day_value)
    ),
    -- Calculate daily averages and coverage from received observations.
    summary AS (
        SELECT
            o.station,
            o.observed_at::date AS observation_date,

            -- Count all rows, then only rows with a PM2.5 value.
            COUNT(*) AS received_hours,
            COUNT(o.pm25) AS valid_pm25_hours,

            -- AVG ignores NULL values.
            ROUND(AVG(o.pm25), 3) AS available_readings_avg_pm25,

            -- Compare available PM2.5 readings with 24 expected hours.
            ROUND(COUNT(o.pm25) * 100.0 / 24, 1)
                AS coverage_percent,
            CASE
                WHEN COUNT(o.pm25) = 24 THEN 'complete'
                WHEN COUNT(o.pm25) = 0 THEN 'no_valid_measurements'
                ELSE 'incomplete'
            END AS coverage_status
        FROM observations AS o

        -- Include all hours of the start and end dates.
        WHERE o.observed_at >= start_date::timestamp
          AND o.observed_at < (end_date + 1)::timestamp
        GROUP BY o.station, o.observed_at::date
    )
    SELECT
        s.station,
        d.observation_date,

        -- Use zero for counts when no matching summary exists.
        COALESCE(summary.received_hours, 0::bigint),
        COALESCE(summary.valid_pm25_hours, 0::bigint),

        -- Leave the average NULL when no PM2.5 values are available.
        summary.available_readings_avg_pm25,
        COALESCE(summary.coverage_percent, 0::numeric),

        -- Label station-days with no received observations.
        CASE
            WHEN summary.station IS NULL
                THEN 'no_received_records'
            ELSE summary.coverage_status
        END

    -- Pair every registered station with every requested day.
    FROM stations AS s
    CROSS JOIN requested_dates AS d

    -- Attach summaries for the same station and date.
    -- Keep the station-day row even when no summary exists.
    LEFT JOIN summary
        ON summary.station = s.station
       AND summary.observation_date = d.observation_date;
$$;