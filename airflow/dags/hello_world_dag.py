from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'financial-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_world():
    print("Hello from Financial Platform!")
    return "Success"

with DAG(
    'example_financial_platform',
    default_args=default_args,
    description='DAG de exemplo',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 23),
    catchup=False,
    tags=['example'],
) as dag:
    
    PythonOperator(
        task_id='say_hello',
        python_callable=hello_world,
    )