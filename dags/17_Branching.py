from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random
from airflow.utils.dates import days_ago

dag = DAG(
    '17_Branching',
    description='DAG de exemplo com Branching',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_view='graph')

def gera_numero_aleatorio():
    return random.randint(1, 10)

task_gera_numero_aleatorio = PythonOperator(
    task_id='tsk_gera_numero_aleatorio',
    python_callable=gera_numero_aleatorio,
    dag=dag)

def le_numero(**context):
    numero = context['task_instance'].xcom_pull(task_ids='tsk_gera_numero_aleatorio')
    if numero % 2 == 0:
        return 'tsk_par'
    else:
        return 'tsk_impar'
    
task_branch = BranchPythonOperator(
    task_id='tsk_branch',
    python_callable=le_numero,
    provide_context=True,
    dag=dag
    )

task_par = BashOperator(
    task_id='tsk_par',
    bash_command='echo "Número Par"',
    dag=dag)

task_impar = BashOperator(
    task_id='tsk_impar',
    bash_command='echo "Número Impar"',
    dag=dag)

task_gera_numero_aleatorio >> task_branch
task_branch >> [task_par, task_impar]
