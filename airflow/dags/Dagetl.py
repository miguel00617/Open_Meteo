from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract import extract_weather
from transform import transform_data
from load import load_data

default_args = {
    "owner": "matias",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="etl_clima_movilidad",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    t3 = PythonOperator(
        task_id="load_postgres",
        python_callable=load_data
    )

    t1 >> t2 >> t3