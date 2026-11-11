# Check database connectivity and count the stored observations.
import psycopg


# "postgres" is the database service name inside the Docker Compose network.
with psycopg.connect(
    host="postgres",
    port=5432,
    dbname="air_quality",
    user="air_quality",
    password="local_dev_password",
    connect_timeout=10,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), COUNT(*) FROM observations")

        # Read the two values returned by the query.
        database, count = cursor.fetchone()

        print(f"Connected to database: {database}")
        print(f"Observation rows: {count}")