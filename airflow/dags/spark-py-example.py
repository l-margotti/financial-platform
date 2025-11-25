from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='spark_pi',
    start_date=datetime(2025, 11, 24),
    schedule=None,
    catchup=False,
    tags=['spark', 'example']
) as dag:
    
    def start_batch():
        print('##### startBatch #####')
    
    def done():
        print('##### done #####')
    
    start_task = PythonOperator(
        task_id='startBatch',
        python_callable=start_batch
    )
    
    spark_task = SparkKubernetesOperator(
        task_id='spark_pi_task',
        namespace='airflow',
        application_file='spark-apps/spark-pi.yaml',
        kubernetes_conn_id='kubernetes_default',
    )
    
    done_task = PythonOperator(
        task_id='done',
        python_callable=done
    )
    
    start_task >> spark_task >> done_task