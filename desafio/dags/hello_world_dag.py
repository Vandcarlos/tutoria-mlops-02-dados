from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Olá, mundo do Airflow!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(task_id="say_hello", python_callable=say_hello)
