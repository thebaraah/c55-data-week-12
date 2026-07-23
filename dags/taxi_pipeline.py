"""Week 12 assignment starter.

Turn this into a scheduled, parameterized, retryable pipeline. The task
list, the file-by-file map, and the point breakdown are in README.md; the
full brief is in the Week 12 "Assignment: Orchestrated Pipeline" chapter.

This starter parses, so `astro dev start` shows the DAG in the UI, but every
task body raises NotImplementedError and the decorator is not configured yet.
Replace the stubs, wire the tasks together, and fill in the decorator. The
autograder fails while any NotImplementedError remains.
"""

import os
import io
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook




# Your per-student schema. AIRFLOW_STUDENT is set in .env for local Astro dev;
# on the shared VM it falls back to the dags/<name>/ directory name.
STUDENT = os.environ.get("AIRFLOW_STUDENT") or Path(__file__).parent.name
SCHEMA = f"airflow_{STUDENT}"
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def find_dbt_dir() -> str:
    """Return the mounted dbt project path (Astro vs shared-VM install root)."""
    for candidate in (
        "/usr/local/airflow/include/dbt_project",  # Astro CLI
        "/opt/airflow/include/dbt_project",        # shared VM docker-compose
    ):
        if Path(candidate).is_dir():
            return candidate
    return "/usr/local/airflow/include/dbt_project"


DBT_DIR = find_dbt_dir()


DBT_ENV = {
    "PG_HOST": "{{ conn.azure_pg.host }}",
    "PG_USER": "{{ conn.azure_pg.login }}",
    "PG_PASSWORD": "{{ conn.azure_pg.password }}",
    "PG_DBNAME": "{{ conn.azure_pg.schema }}",
    "PG_SCHEMA": SCHEMA,
}

def _partition_date():

    context = get_current_context()

    dag_run = context["dag_run"]

    date = dag_run.logical_date or dag_run.run_after

    return date.strftime("%Y-%m-%d")



@dag(
    # TODO Task 1 (see README): configure the decorator.
    dag_id="baraah_taxi_pipeline",
    schedule="@monthly",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["week12","taxi", "student:baraah"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },

)
def taxi_pipeline():
    @task
    def ingest_taxi_month() -> int:

        ds = _partition_date()

        year_month = ds[:7]

        print(f"Processing month: {year_month}")

        url = (
            f"{TLC_BASE}/"
            f"green_tripdata_{year_month}.parquet"
        )
        #download parquet
        response = requests.get(
            url,
            timeout=60
        )

        response.raise_for_status()

        #parquet to dataframe
        df = pd.read_parquet(
            io.BytesIO(response.content)
        )


        hook = PostgresHook(
            postgres_conn_id="azure_pg"
        )


        engine = hook.get_sqlalchemy_engine()
        #create schema
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'
                )
        #create table if missing
        df.head(0).to_sql(
            "raw_trips",
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
        )
         
        # idempotency remove old data for same month
        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    f"""
                    DELETE FROM "{SCHEMA}".raw_trips
                    WHERE to_char(
                        lpep_pickup_datetime,
                        'YYYY-MM'
                    ) = %s
                    """,
                    (year_month,),
                )
        #insert fresh data        
        df.to_sql(
            "raw_trips",
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
        )


        return len(df)




    dbt = (
        "uvx --python 3.11 "
        "--from 'dbt-core==1.10.*' "
        "--with 'dbt-postgres==1.10.*' "
        "dbt"
    )


    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{dbt} deps "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} && "
            f"{dbt} run "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR}"
        ),
        env=DBT_ENV,
        append_env=True,
    )


    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{dbt} test "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR}"
        ),
        env=DBT_ENV,
        append_env=True,
    )


    ingest_taxi_month() >> dbt_run >> dbt_test
    """Download one month of TLC green-taxi data and load it into
            ``{SCHEMA}.raw_trips`` idempotently. Return the number of rows.
    
            TODO Task 2 and Task 3 (see README).
            """

    # TODO Task 2 (see README): add the two transform tasks, wire the full
    # chain, and run the transform through the Chapter 4 command so it works
    # on the image's Python. TODO Task 4: add retry behaviour.


taxi_pipeline()
