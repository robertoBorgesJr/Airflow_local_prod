from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

dag = DAG(
    '20_dataset_producer', 
    description='Producer of Churn_producer.csv Dataset',
    schedule_interval=None, 
    start_date=datetime(2025, 8, 27),
    catchup=False,
    default_view='graph'
)

Dataset = Dataset('/opt/airflow/data/Churn_producer.csv')

def produce_dataset():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_producer.csv', sep=';', index=False)

tsk_produce = PythonOperator(
    task_id='tsk_produce',
    python_callable=produce_dataset,
    dag=dag,
    outlets=[Dataset]
)