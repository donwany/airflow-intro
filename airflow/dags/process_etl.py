from datetime import datetime
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator


# ------------------------
# Extract
# ------------------------
def extract(ti):
    sales = [
        {"customer": "Alice", "product": "Laptop", "price": 1200, "quantity": 1},
        {"customer": "Bob", "product": "Mouse", "price": 25, "quantity": 3},
        {"customer": "Charlie", "product": "Keyboard", "price": 80, "quantity": 2},
    ]

    print("Extracted Data:")
    print(sales)

    ti.xcom_push(key="sales_data", value=sales)


# ------------------------
# Transform
# ------------------------
def transform(ti):
    sales = ti.xcom_pull(task_ids="extract", key="sales_data")

    transformed = []

    for sale in sales:
        total = sale["price"] * sale["quantity"]

        discount = 0.10 if total > 100 else 0

        final_price = total * (1 - discount)

        transformed.append({
            "customer": sale["customer"],
            "product": sale["product"],
            "quantity": sale["quantity"],
            "total": total,
            "discount": discount,
            "final_price": round(final_price, 2)
        })

    print("Transformed Data:")
    print(transformed)

    ti.xcom_push(key="transformed_data", value=transformed)


# ------------------------
# Load
# ------------------------
def load(ti):
    data = ti.xcom_pull(task_ids="transform", key="transformed_data")

    filename = "/opt/airflow/data/sales_report.csv"

    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=data[0].keys()
        )

        writer.writeheader()
        writer.writerows(data)

    print(f"Report written to {filename}")


with DAG(
    dag_id="process_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["ETL", "Python"],
):

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

    extract_task >> transform_task >> load_task