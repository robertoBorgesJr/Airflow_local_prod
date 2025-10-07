from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable

dag = DAG(
    '15_variable',
    description='DAG de exemplo com Variable',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_view='graph')

def print_variable(**context):
    path = Variable.get("mainPath")
    print(f'O valor da variável é: {path}')

task1 = PythonOperator(
    task_id='tsk1',
    python_callable=print_variable,
    dag=dag
)

task1
