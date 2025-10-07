from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

dag = DAG(
    '14_dummy',
    description='DAG de exemplo com DummyOperator',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_view='graph')

task1 = BashOperator(
    task_id='tsk1',
    bash_command='sleep 5',
    dag=dag) 

task2 = BashOperator(
    task_id='tsk2',
    bash_command='sleep 5',
    dag=dag)

task3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag)

task4 = BashOperator(
    task_id='tsk4',
    bash_command='sleep 5',
    dag=dag) 

task5 = BashOperator(
    task_id='tsk5',
    bash_command='sleep 5',
    dag=dag)

taskDummy = DummyOperator(task_id='taskDummy', dag=dag)

# [task1, task2, task3] >> [task4, task5] # o airflow não suporta essa forma
[task1, task2, task3] >> taskDummy >> [task4, task5] # Usando DummyOperator como intermediário
