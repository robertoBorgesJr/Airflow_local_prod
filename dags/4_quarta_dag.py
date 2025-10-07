from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

## usando o contexto do gerenciador de contexto (with), não é necessário passar o dag=dag em cada tarefa
with DAG(
    '04_quarta_dag',
    description='Minha quarta DAG',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False) as dag:

    task1 = BashOperator(
        task_id='tsk1',
        bash_command='sleep 5')

    task2 = BashOperator(
        task_id='tsk2',
        bash_command='sleep 5')

    task3 = BashOperator(
        task_id='tsk3',
        bash_command='sleep 5')

    task1.set_downstream(task2)  # task1 >> task2
    task2.set_downstream(task3)  # task2 >> task3

    ## o resultado final é o mesmo que:
    #task1 >> task2 >> task3