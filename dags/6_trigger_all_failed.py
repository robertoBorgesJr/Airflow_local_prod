from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    '06_trigger_all_failed',
    description='Trigger all_failed',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False)

task1 = BashOperator(
    task_id='tsk1',
    bash_command='exit 1',  # Comando que força uma falha
    dag=dag)

task2 = BashOperator(
    task_id='tsk2',
    bash_command='exit 1', # Comando que força uma falha
    dag=dag)

task3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag,
    trigger_rule='all_failed') # Executa se todas as tarefas anteriores falharem

[task1,task2] >> task3