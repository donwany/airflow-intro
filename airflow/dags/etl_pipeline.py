from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    print("Extracting data...")


def transform():
    print("Transforming data...")


def load():
    print("Loading data into database...")


with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="*/1 * * * *", # @hourly, @daily, @weekly, @monthly, @yearly
    catchup=False,
    tags=["tutorial", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
