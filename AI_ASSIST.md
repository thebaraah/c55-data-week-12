# AI assistance log

<!-- Document at least one point where you used an LLM on this assignment.
     Never paste connection strings, passwords, or real data. Replace TODO. -->

## Use 1

**Prompt I sent:** 
I received an Airflow task failure with the error:
AirflowNotFoundException: The conn_id azure_pg isn't defined

I asked the LLM to explain the cause of this error and what steps were needed to fix the Airflow PostgreSQL connection.


**What the model answered:** 
The model explained that the DAG was trying to use an Airflow connection named `azure_pg` through `PostgresHook`but this connection hadnt been created in the Airflow environment. It suggested checking the Airflow connections and adding the missing connection using the Astro CLI.

**What I kept, changed, or discarded, and why:** 
I kept the explanation that the issue was caused by a missing Airflow connection. I verified the solution by creating the `azure_pg` connection in my Astro Airflow environment and rerunning the DAG. The task succeeded afterward.
I configured my own environment variables and connection settings.
