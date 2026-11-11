# Check that Python can reach PostgreSQL and count stored observations.
# This script reads data; it does not insert, update or delete observations.
import psycopg


# Connect using the local database settings from compose.yaml.
# The hostname "postgres" works inside the Docker Compose network.
# Allow up to 10 seconds to establish the connection.
with psycopg.connect(
    host="postgres",
    port=5432,
    dbname="air_quality",
    user="air_quality",
    password="local_dev_password",
    connect_timeout=10,
) as connection:
    # A cursor lets us send SQL commands and read their results.
    with connection.cursor() as cursor:
        # Request the database name and the number of rows in observations.
        cursor.execute("SELECT current_database(), COUNT(*) FROM observations")

        # The query returns one row containing two values.
        # Store them in the variables "database" and "count".
        database, count = cursor.fetchone()

        # Display the result in the terminal.
        print(f"Connected to database: {database}")
        print(f"Observation rows: {count}")

# The "with" blocks close the cursor and connection when finished.