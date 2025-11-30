"""DAG para coletar dados da Binance"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta


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
        application_file='spark-apps/binance-collector.yaml',
        kubernetes_conn_id='kubernetes_default',
    )