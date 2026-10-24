CREATE TABLE IF NOT EXISTS observations (
    station TEXT NOT NULL,
    observed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    pm25 NUMERIC,
    pm10 NUMERIC,
    so2 NUMERIC,
    no2 NUMERIC,
    co NUMERIC,
    o3 NUMERIC,

    temperature NUMERIC,
    pressure NUMERIC,
    dew_point NUMERIC,
    rainfall NUMERIC,
    wind_direction TEXT,
    wind_speed NUMERIC,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (station, observed_at)
);