from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG(
    '10_dag_run_dag',
    description='Dag principal que roda outra dag',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_view='graph')

task1 = BashOperator(
    task_id='tsk1',
    bash_command='sleep 5',
    dag=dag)

task2 = TriggerDagRunOperator(
    task_id='tsk2',
    trigger_dag_id='9_dag_run_dag2',  # Nome da DAG que será disparada
    dag=dag)


task1 >> task2
