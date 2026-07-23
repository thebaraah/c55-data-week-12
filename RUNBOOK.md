# RUNBOOK

<!-- Replace every TODO with real content. Another student should be able to
     operate your DAG from this file alone, without reading your Python. -->

## How to trigger the DAG manually

1. Start the local Astro Airflow environment:

```bash
astro dev start

## How to run a backfill
The DAG uses a monthly schedule, so backfills should run month by month.

astro dev run backfill create \
  --dag-id taxi_pipeline \
  --from-date 2024-01-01 \
  --to-date 2024-07-31 \
  --max-active-runs 1

After the backfill finishes, verify the processed rows in PostgreSQL:

SELECT
    to_char(lpep_pickup_datetime, 'YYYY-MM') AS month,
    COUNT(*) AS rows
FROM airflow_baraah.raw_trips
GROUP BY 1
ORDER BY 1;
## How to inspect task logs

1. open the UI airflow 
2. open taxi_pipeline
3. slect the run DAG and then click the task 
ingest_taxi_month
dbt_run
dbt_test
4. open logs

## Top 3 likely failures and first response

1. TLC data download failure (HTTP 403 or missing parquet file)

Symptom: ingest_taxi_month fails with an HTTP error.
First check: Inspect the task log and verify the logical date used for the run.
Fix: Use a valid historical logical date where the TLC parquet file exists (for example 2024-01-01).

2. PostgreSQL connection failure
Symptom: Error such as The conn_id azure_pg isn't defined.
First check: Verify the Airflow connection exist.
Fix: Add or update the PostgreSQL connection.

3. dbt run or dbt test failure
Symptom: dbt_run or dbt_test task becomes failed.
First check: Open the task logs and check the dbt error message.
Fix: Verify that the dbt project exists under include/dbt_project ,
and that dbt is executed through: uvx --python 3.11
