# Assignment report

<!-- Replace every TODO. Keep it short: a few sentences per section. -->

## Schedule choice and reason

 we did a monthly schedule (@monthly) because the TLC taxi dataset is partitioned by month. Each DAG run processes one month of data based on Airflow logical_date.

## Task dependency graph

describe the chain (ingest -> dbt_run -> dbt_test) and why the order matters.

ingest_taxi_month -> dbt_run -> dbt_test

The ingest task runs first because it downloads and loads the raw taxi data into PostgreSQL. After the raw data is available, 'dbt_run' transforms the data using the the class reference. Finally, 'dbt_test' validates the dbt models to ensure data quality.

ingest loads raw data
dbt transforms data
dbt tests validate models.

## dbt project used

the class reference

## One debugging case I resolved

what failed, how you found the cause in the logs, and the fix.

The first manual run failed because the DAG used the current date and tried downloading green_tripdata_2026-07.parquet, which was unavailable I think .

I fixed this by triggering the DAG with a logical date (2024-01-01) and using Airflow logical_date instead of datetime.now().

The DAG uses Airflow's logical date to derive the year-month partition.

The ingest task reads dag_run.logical_date and converts it to YYYY-MM format to build the TLC parquet URL.

Backfill command used:
astro dev run backfill create \
  --dag-id taxi_pipeline \
  --from-date 2024-01-01 \
  --to-date 2024-07-31 \
  --max-active-runs 1

## shared Airflow deploy PR
  https://github.com/lassebenni/c55-shared-airflow/pull/7


<!-- Target tier: also document your {{ ds }} parameter usage and the
     backfill command(s) you ran, with before/after row counts. -->
