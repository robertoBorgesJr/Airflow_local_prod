from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

dag = DAG(
    '16_Pools',
    description='DAG de exemplo com Pools',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_view='graph')

task1 = BashOperator(
    task_id='tsk1',
    bash_command='sleep 5',
    dag=dag,
    pool='specifcPool',
    priority_weight=3) 

task2 = BashOperator(
    task_id='tsk2',
    bash_command='sleep 5',
    dag=dag,
    pool='specifcPool',
    priority_weight=6)

task3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag,
    pool='specifcPool',
    priority_weight=1)

task4 = BashOperator(
    task_id='tsk4',
    bash_command='sleep 5',
    dag=dag,
    pool='specifcPool',
    priority_weight=2) 
