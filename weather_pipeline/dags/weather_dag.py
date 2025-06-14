from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import extract_weather
from etl.transform import transform_weather
from etl.load import load_weather

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def run_etl():
    df = extract_weather("Bangkok")
    if df.empty:
        print("No data extracted.")
        return
    df = transform_weather(df)
    load_weather(df)

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    # schedule_interval="@hourly",
    schedule_interval="* * * * *",  # รันทุก 1 นาที
    catchup=False,
) as dag:
    etl_task = PythonOperator(
        task_id="run_weather_etl",
        python_callable=run_etl,
    )

    etl_task
