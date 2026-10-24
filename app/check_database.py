import psycopg


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
        database, count = cursor.fetchone()

        print(f"Connected to database: {database}")
        print(f"Observation rows: {count}")