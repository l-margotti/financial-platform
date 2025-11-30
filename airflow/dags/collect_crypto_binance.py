"""DAG para coletar dados da Binance"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from utils.spark_helper import SparkJobBuilder


with DAG(
    'collect_crypto_binance',
    description='Coleta dados da Binance API',
    start_date=datetime(2025, 11, 30),
    schedule='0 * * * *',
    catchup=False,
    tags=['crypto', 'binance', 'bronze'],
    default_args={'owner': 'data-engineering', 'retries': 2}
) as dag:
    
    collect_data = SparkKubernetesOperator(
        task_id='collect_binance_data',
        namespace='airflow',
        body=SparkJobBuilder('binance-{{ ts_nodash }}', 'collect_binance_data.py')
            .with_arguments(['s3a://bronze/crypto/binance'])
            .with_resources(driver_memory="512m", executor_instances=1, executor_memory="512m")
            .with_labels({'layer': 'bronze', 'source': 'binance'})
            .build(),
        kubernetes_conn_id='kubernetes_default',
    )