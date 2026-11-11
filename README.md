# Air-Quality Streaming Pipeline

IU Project: Data Engineering (DLBDSEDE02)

## Purpose and usage

This prototype simulates a municipal sensor network delivering hourly
air-quality observations.

Environmental analysts can query daily PM2.5 averages by station and date.
Each result includes received-record counts, valid-reading counts and
coverage against 24 expected hourly measurements.

By default, comparisons include only station-days with 24 valid PM2.5
readings. Incomplete results remain available for investigation.
Municipal planners could use reviewed comparisons to prioritise further
environmental investigation.

This completeness rule is a conservative prototype choice, not an official
air-quality reporting standard. Complete coverage does not establish
sensor accuracy or eliminate sampling bias.

## Architecture

CSV files → Python producer → Kafka → Python consumer → PostgreSQL

- Producer: replays observations in timestamp order at configurable intervals.
- Kafka: retains messages independently of database availability.
- Consumer: validates records, converts missing values and writes to PostgreSQL.
- PostgreSQL: stores observations and calculates reports on demand.
- Docker Compose: coordinates application services and initialization.

The prototype uses one Kafka broker with replication factor 1.

## Requirements

- Docker Desktop, or Docker Engine with Docker Compose
- Git
- The 12 dataset CSV files described below

Python and its dependencies run inside Docker.

## Dataset setup

Download and extract the Beijing Multi-Site Air Quality dataset:

https://www.kaggle.com/datasets/sid321axn/beijing-multisite-airquality-data-set

Original dataset and citation:

Chen, S. (2017). Beijing Multi-Site Air Quality [Dataset].
UCI Machine Learning Repository.
https://doi.org/10.24432/C5RK5G

The original repository lists the dataset under CC BY 4.0.

Create a folder named `data` in the project root. Place all 12 original
`PRSA_Data_*.csv` files directly inside it, without an enclosing subfolder.

The dataset contains 420,768 hourly station observations from
1 March 2013 to 28 February 2017. Source files are not included in Git.

## Start the pipeline

From the project root:

```bash
docker compose up -d --build
```

On fresh storage, this automatically:

1. Creates the database table, station registry, view and reporting function.
2. Starts Kafka and creates `air-quality-observations`.
3. Starts the continuous database consumer.
4. Replays a default sample of 24 observations.

Inspect services and logs:

```bash
docker compose ps -a
docker compose logs --tail 30 producer consumer
```

The setup service and sample producer should exit with code 0.
Kafka, PostgreSQL and the consumer remain running.

Check stored observations:

```bash
docker compose exec postgres psql -U air_quality -d air_quality -P pager=off -c "SELECT COUNT(*) FROM observations;"
```

A fresh default run should eventually contain 24 rows.
These 24 observations cover two hours across 12 stations, not a full day.
Daily reports will therefore show incomplete coverage until more
observations are replayed.

## Replay a larger sample

Replay the first five days across all 12 stations:

```bash
docker compose run --rm producer python producer.py --limit 1440 --interval 0
```

`--limit` controls the total number of observations across all stations.
`--interval` controls the delay in seconds between messages.

Each replay starts from the beginning of the files. Existing station-hour
records are ignored by the consumer, preventing duplicate database rows.

## Query daily results

```bash
docker compose exec postgres psql -U air_quality -d air_quality -P pager=off -c "SELECT * FROM report_daily_pm25(DATE '2013-03-04', DATE '2013-03-05') ORDER BY observation_date, station;"
```

Both dates are included. Load the five-day sample first to populate this period.

Run the example comparison and excluded-results queries:

```bash
docker compose exec -T postgres psql -U air_quality -d air_quality -P pager=off -v ON_ERROR_STOP=1 < sql/003_station_comparison.sql
```

PM2.5 averages are in µg/m³. SQL NULL means unavailable, not zero.
In psql output, NULL is normally displayed as a blank cell.

Report statuses:

- `complete`: 24 valid PM2.5 readings.
- `incomplete`: 1–23 valid PM2.5 readings.
- `no_valid_measurements`: records exist, but all PM2.5 values are missing.
- `no_received_records`: no records exist for the expected station-day.

Reports assume all 12 registered stations are active throughout the
requested period. They describe currently stored data and do not declare
an incomplete day permanently final.

## Correctness and recovery

- Observation timestamps represent Beijing local time.
- Ingestion timestamps are recorded separately.
- The station–timestamp primary key prevents duplicate rows.
- Missing CSV measurements become SQL NULL.
- Invalid station names, dates and selected invalid measurements stop processing.
- Kafka progress is committed only after a successful database transaction.
- Database connection failures are retried five times.
- Docker restarts the consumer if it exits with an error.
- The producer waits and retries when the expected source files cannot
  be found or opened before replay.
- Named volumes preserve Kafka and PostgreSQL data across container recreation.

A persistent invalid message blocks consumer progress and requires investigation.
Recovery from source read errors during replay has not been implemented.
The single broker provides no replicated protection against storage loss.
Kafka buffering is limited by retention settings and available disk space.
Named volumes are not backups and do not prevent file corruption.
Recovery from damaged Kafka storage may require manual intervention.

Database credentials in Compose are local prototype credentials.

## Verification performed

- End-to-end streaming and storage.
- Sample field values and NA-to-NULL conversion.
- Duplicate replay without additional database rows.
- Recovery following a database outage and manual consumer restart.
- Automatic consumer recovery following a database outage.
- Source availability recovery before replay, without restarting the producer.
- Daily averages, incomplete coverage and absent station-days.
- Reports updating when additional historical observations arrive.
- Fresh database, Kafka and complete local startup.
- Validation of all 420,768 source rows: no failures under current rules.
- Query-plan checks on the 1,440-row stored sample.
- Reporting with 24 received records but no valid PM2.5 measurements,
  verified using a rolled-back transaction.
- Kafka topic end offset and committed consumer offset preserved across
  broker container recreation with its named volume retained.

On 11 November 2026, Kafka failed to start because its
replication-offset-checkpoint file contained invalid data. Kafka storage
was backed up and the archive's readability checked before the damaged
checkpoint was renamed. Kafka then recreated the checkpoint and started
successfully. The original cause of the damage remains unknown.

After recovery, 24 observations were replayed and recognised as duplicates.
PostgreSQL retained its 1,440 observations, and the consumer reached zero
lag at offset 3038. Old topic messages and the previous consumer-group
bookmark were unavailable; retention expiry was a possible explanation
but was not conclusively established. This was manual recovery, not an
automatic repair performed by the pipeline.

A clean local Git clone was tested successfully with fresh volumes and the documented dataset setup. Full-dataset ingestion, concurrent-user load, GitHub cloning and execution on another machine remain untested.

Run validation checks:

```bash
docker compose run --no-deps --build --rm producer python check_validation.py
docker compose run --no-deps --rm producer python check_dataset.py
```

Check database connectivity and the stored row count:

```bash
docker compose up -d postgres
docker compose run --no-deps --build --rm producer python check_database.py
```

Test reporting when all 24 received PM2.5 readings are missing:

```bash
docker compose exec -T postgres psql -U air_quality -d air_quality -P pager=off -v ON_ERROR_STOP=1 < sql/006_missing_pm25_test.sql
```

The reporting test inserts artificial observations inside a transaction
and rolls them back afterwards.

Inspect query plans:

```bash
docker compose exec -T postgres psql -U air_quality -d air_quality -P pager=off -v ON_ERROR_STOP=1 < sql/005_query_performance.sql
```

## Stop the pipeline

```bash
docker compose down
```

This removes the project containers and network while retaining named volumes.
Do not add `--volumes` unless you intend to delete the stored project data.

Initialization SQL scripts run only on an empty database volume.
Editing those files does not automatically update an existing database.