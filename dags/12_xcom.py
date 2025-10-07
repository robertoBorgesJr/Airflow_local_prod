from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator

dag = DAG(
    '12_xcom',
    description='DAG de exemplo com XCom',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_view='graph')

def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valor_xcom1', value=10200)


task1 = PythonOperator(
    task_id='tsk1',
    python_callable=task_write,
    dag=dag
)

def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valor_xcom1')
    print(f'O valor lido foi: {valor}')

task2 = PythonOperator(
    task_id='tsk2',
    python_callable=task_read,
    dag=dag
)

task1 >> task2
