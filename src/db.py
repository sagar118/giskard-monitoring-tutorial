import psycopg

create_table_query = """
create table if not exists data_drift_metrics (
    timestamp timestamp,
    holiday_drift_score float,
    weathersit_drift_score float,
    temp_drift_score float,
    atemp_drift_score float,
    windspeed_drift_score float,
    target_drift_score float,
    dataset_drift_score float,
    imp_var_drift_score float
);
"""

def prep_db():
    """
    Prepare Database
    Ensure that the PostgreSQL database 'giskard_monitoring' is created and ready for use.

    """
    # Connect to the PostgreSQL database and create the table if it does not exist
    with psycopg.connect(
        "host=localhost port=5432 user=postgres password=postgres", autocommit=True
    ) as conn:
        # Check if the database exists
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname = 'giskard_monitoring'")

        # Create the database if it does not exist
        if len(res.fetchall()) == 0:
            conn.execute("CREATE DATABASE giskard_monitoring;")

        # Create the table if it does not exist
        with psycopg.connect(
            "host=localhost port=5432 user=postgres password=postgres dbname=giskard_monitoring",
            autocommit=True,
        ) as conn:
            conn.execute(create_table_query)

def insert_into_db(cursor, metrics, begin_timestamp):

    # Insert the metrics into the PostgreSQL database
    cursor.execute(
        """
        INSERT INTO data_drift_metrics (
            timestamp,
            holiday_drift_score,
            weathersit_drift_score,
            temp_drift_score,
            atemp_drift_score,
            windspeed_drift_score,
            target_drift_score,
            dataset_drift_score,
            imp_var_drift_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            begin_timestamp, 
            metrics["holiday_drift_score"],
            metrics["weathersit_drift_score"],
            metrics["temp_drift_score"],
            metrics["atemp_drift_score"],
            metrics["windspeed_drift_score"],
            metrics["target_drift_score"],
            metrics["dataset_drift_score"],
            metrics["imp_var_drift_score"],
        ),
    )