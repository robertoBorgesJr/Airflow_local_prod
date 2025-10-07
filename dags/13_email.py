from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False, # A tarefa não depende do sucesso da execução anterior
    'start_date': datetime(2025, 8, 1),
    'email': ['borgesjoy@gmail.com'], # Email para notificações
    'email_on_failure': False,
    'email_on_retry': False, 
    'retries': 1, # Número de tentativas em caso de falha
    'retry_delay': timedelta(seconds=15), # Tempo entre tentativas    
    }

dag = DAG(
    '13_email',
    description='DAG com envio de email',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['email'],
    default_view='graph'
    )

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
    bash_command='exit 1',  # força uma falha
    dag=dag) 

task5 = BashOperator(
    task_id='tsk5',
    bash_command='sleep 5',
    dag=dag,
    trigger_rule='none_failed')  # Executa se nenhuma tarefa anterior falhar

task6 = BashOperator(
    task_id='tsk6',
    bash_command='sleep 5',
    dag=dag,
    trigger_rule='none_failed')  # Executa se nenhuma tarefa anterior falhar)

send_email = EmailOperator(
    task_id='send_email',
    to='borgesjoy@gmail.com',
    subject='Airflow Error',
    html_content="""<h3>Ocorreu um erro na Dag. </h3>
                    <p>Dag: send_email </p> 
                 """,
    dag=dag, 
    trigger_rule='one_failed'  # Executa se pelo menos uma tarefa anterior falhar
)


[task1,task2] >> task3 >> task4 >> [task5,task6, send_email]
