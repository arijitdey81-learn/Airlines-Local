"""
OpenSky Flights DAG
-------------------
Daily pipeline that extracts flight arrivals and departures from the
OpenSky Network API for a set of global airports, then loads the raw
data into Snowflake for downstream dbt modelling.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Make the code in /opt/airflow/include importable
sys.path.insert(
    0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include")
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "analytics-academy",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="opensky_flights_to_snowflake",
    default_args=default_args,
    description="Extract OpenSky flight data and load into Snowflake",
    schedule="@daily",
    start_date=datetime(2025, 3, 15),
    catchup=False,
    max_active_runs=1,
    tags=["opensky", "snowflake", "raw"],
)
def opensky_flights_dag():

    @task()
    def extract_flights(data_interval_start=None, data_interval_end=None):
        """Pull the day's arrivals and departures and save them to a file."""
        from opensky_client import get_access_token, fetch_all_airports

        begin = int(data_interval_start.timestamp())
        end = int(data_interval_end.timestamp()) - 1

        token = get_access_token()
        flights = fetch_all_airports(token, begin, end)

        path = f"/tmp/opensky_{begin}.json"
        with open(path, "w") as f:
            json.dump(flights, f)

        logger.info("Fetched %d flights, saved to %s", len(flights), path)
        return path  # only the path goes through XCom, not the data

    @task()
    def load_to_snowflake(path):
        """Load the extracted file into Snowflake RAW_FLIGHTS (no duplicates)."""
        from snowflake_loader import (
            get_snowflake_connection,
            create_raw_table_if_not_exists,
            load_flights,
        )

        with open(path) as f:
            flights = json.load(f)

        conn = get_snowflake_connection()
        try:
            create_raw_table_if_not_exists(conn)
            new_rows = load_flights(conn, flights)
        finally:
            conn.close()

        os.remove(path)
        return new_rows

    @task()
    def log_summary(new_rows):
        logger.info("Pipeline complete - %d new flight records loaded.", new_rows)

    path = extract_flights()
    new_rows = load_to_snowflake(path)
    log_summary(new_rows)


opensky_flights_dag()