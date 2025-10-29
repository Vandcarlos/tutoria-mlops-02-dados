import src.mlflow_utils as mlflow_utils
import src.parquet_utils as parquet_utils
import pandas as pd
import mlflow

from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

TARGET_TABLE = "dataset_etl"

def load(run_id: str):
    with mlflow_utils.use_run(run_id=run_id):
        hook = PostgresHook(postgres_conn_id="PG_DWH")
        engine = create_engine(hook.get_uri())

        df = parquet_utils.load_data_frame(name="clear")

        with engine.begin():
            df.to_sql(
                TARGET_TABLE,
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        mlflow.log_metric("rows_loaded", len(df))
