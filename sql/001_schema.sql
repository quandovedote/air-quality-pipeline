-- Store one observation per station and observation time.
CREATE TABLE IF NOT EXISTS observations (
    station TEXT NOT NULL,

    -- Original measurement time, interpreted as Beijing local time.
    observed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    -- Pollutant measurements; NULL means a measurement is missing.
    pm25 NUMERIC,
    pm10 NUMERIC,
    so2 NUMERIC,
    no2 NUMERIC,
    co NUMERIC,
    o3 NUMERIC,

    -- Weather measurements; these also allow missing values.
    temperature NUMERIC,
    pressure NUMERIC,
    dew_point NUMERIC,
    rainfall NUMERIC,
    wind_direction TEXT,
    wind_speed NUMERIC,

    -- Automatically record when the observation is stored.
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate station-time pairs and support searches by station/time.
    PRIMARY KEY (station, observed_at)
);