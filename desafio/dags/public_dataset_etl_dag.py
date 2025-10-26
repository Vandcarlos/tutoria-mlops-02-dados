import sys, os
sys.path.append("/opt/airflow")  # garante acesso ao src
import mlflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.public_dataset.extract import extract
from src.public_dataset.transform import transform
from src.public_dataset.load import load

DATASET_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"

with DAG(
    dag_id="etl_public_dataset_dag",
    description="Pipeline ETL de dataset público",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "public"],
) as dag:
    mlflow
    t1 = PythonOperator(task_id="extract", python_callable=extract_task)
    t2 = PythonOperator(task_id="transform", python_callable=transform_task)
    t3 = PythonOperator(task_id="load", python_callable=load_task)
    t1 >> t2 >> t3

def extract_task(**context):
    df = extract(DATASET_URL)
    context["ti"].xcom_push(key="df_shape", value=df.shape)
    df.to_csv("/opt/airflow/data/tmp_extract.csv", index=False)

def transform_task(**context):
    import pandas as pd
    df = pd.read_csv("/opt/airflow/data/tmp_extract.csv")
    df_clean = transform(df)
    df_clean.to_csv("/opt/airflow/data/tmp_transform.csv", index=False)
    context["ti"].xcom_push(key="clean_shape", value=df_clean.shape)

def load_task(**context):
    import pandas as pd
    df_clean = pd.read_csv("/opt/airflow/data/tmp_transform.csv")
    load(df_clean)