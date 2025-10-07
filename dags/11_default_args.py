from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False, # A tarefa não depende do sucesso da execução anterior
    'start_date': datetime(2025, 8, 1),
    'email': ['borgesjoy@gmail.com'], # Email para notificações
    'email_on_failure': False,
    'email_on_retry': False, 
    'retries': 1, # Número de tentativas em caso de falha
    'retry_delay': timedelta(seconds=15), # Tempo entre tentativas
    'default_view':'graph'
    }

dag = DAG(
    '11_default_args',
    description='DAG com default args',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=['processo', 'tag', 'pipeline']
    )

task1 = BashOperator(
    task_id='tsk1',
    bash_command='sleep 5',
    dag=dag,
    retries=3)  # Sobrescreve o número de tentativas para esta tarefa

task2 = BashOperator(
    task_id='tsk2',
    bash_command='sleep 5',
    dag=dag)

task3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag)


task1 >> task2 >> task3
