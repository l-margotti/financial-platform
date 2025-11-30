"""
DAG simples para coletar dados da Binance a cada 1 hora.
Armazena dados brutos (bronze) no MinIO.
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta
from utils.spark_helper import SparkJobBuilder


default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    dag_id='collect_crypto_binance',
    description='Coleta dados de criptomoedas da Binance API',
    default_args=default_args,
    start_date=datetime(2025, 11, 30),
    schedule='0 * * * *',  # A cada 1 hora
    catchup=False,
    tags=['crypto', 'binance', 'bronze', 'ingestion'],
    max_active_runs=1,
) as dag:
    
    # Coleta dados da Binance
    collect_data = SparkKubernetesOperator(
        task_id='collect_binance_data',
        namespace='airflow',
        body=SparkJobBuilder('binance-collector-{{ ts_nodash }}', 'collect_binance_data.py')
            .with_arguments(['s3a://bronze/crypto/binance'])
            .with_resources(
                driver_memory="512m",
                executor_instances=1,
                executor_memory="512m"
            )
            .with_labels({'layer': 'bronze', 'source': 'binance'})
            .build(),
        kubernetes_conn_id='kubernetes_default',
    )
