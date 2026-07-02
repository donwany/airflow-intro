from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="spark_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="*/2 * * * *",
    catchup=False
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting Spark ETL Pipeline'"
    )

    run_spark_job = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
        spark-submit --master local[*] /opt/airflow/spark_jobs/spark_etl_job.py
        """
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'ETL Pipeline Completed'"
    )

    start >> run_spark_job >> end