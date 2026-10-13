"""End-to-end pipeline: fetch -> Spark clean -> load to Snowflake -> dbt build.
 
Runs entirely inside the Airflow container (pyspark is pip-installed there
too), connecting to the Spark cluster as a remote client via
spark://spark-master:7077 — the same master URL already hardcoded in the job
scripts, so no spark-submit binary is required, just plain python script.py.
 
Note: this differs from manually `docker compose exec spark-master
spark-submit ...` (used earlier for ad-hoc debugging) — if the client-mode
driver can't be reached back by the executors from this container, the fix
is usually setting spark.driver.host in the job script to this container's
hostname. Not yet hit this failure mode in testing, flagging just in case.
"""
from datetime import datetime
 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
 
DBT_PROJECT_DIR = "/opt/airflow/dbt"
JOBS_DIR = "/opt/airflow/jobs"
 
with DAG(
    dag_id="capstone_fda_drug_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually for the capstone
    catchup=False,
    tags=["capstone", "fda", "drug"],
) as dag:
 
    with TaskGroup("fetch") as fetch_group:
        fetch_drug_ndc = BashOperator(
            task_id="fetch_drug_ndc",
            bash_command=f"python {JOBS_DIR}/fetch/fetch_drug_ndc.py",
        )
 
        fetch_drug_adverse_events = BashOperator(
            task_id="fetch_drug_adverse_events",
            bash_command=f"python {JOBS_DIR}/fetch/fetch_drug_adverse_events.py",
        )
 
    with TaskGroup("spark_clean") as spark_clean_group:
        spark_clean_drug_ndc = BashOperator(
            task_id="spark_clean_drug_ndc",
            bash_command=f"python {JOBS_DIR}/spark/clean_drug_ndc.py",
        )
 
        spark_clean_drug_adverse_events = BashOperator(
            task_id="spark_clean_drug_adverse_events",
            bash_command=f"python {JOBS_DIR}/spark/clean_drug_adverse_events.py",
        )
 
    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command=f"python {JOBS_DIR}/load/load_to_snowflake.py",
    )
 
    with TaskGroup("dbt_build") as dbt_build_group:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
        )
 
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging",
        )
 
        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
        )
 
        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts",
        )
 
        dbt_run_quarantine_reporting = BashOperator(
            task_id="dbt_run_quarantine_reporting",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select quarantine reporting",
        )
 
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        )
 
        dbt_deps >> dbt_run_staging >> dbt_snapshot >> dbt_run_marts >> dbt_run_quarantine_reporting >> dbt_test
 
    # Cross-group edges: pair each fetch with its matching spark-clean task
    # individually (not group >> group) since it's a 1:1 pairing, not fan-out
    fetch_drug_ndc >> spark_clean_drug_ndc
    fetch_drug_adverse_events >> spark_clean_drug_adverse_events
    spark_clean_group >> load_to_snowflake >> dbt_build_group