from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

dag = DAG(
    '23_Hooks',
    description='Exemplo de uso de Hooks',
    schedule_interval=None,
    start_date=datetime(2025, 9, 2),
    catchup=False,
    default_view='graph'
)

def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('create table if not exists clientes (id serial primary key, nome varchar(50), idade int, salario float);', 
                autocommit=True)
    
def insert_data():    
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("insert into clientes (nome, idade, salario) values ('João Hook', 30, 5000.00);")

def select_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    records = pg_hook.get_records('select * from clientes;')        
    kwargs['ti'].xcom_push(key='query_result', value = records)

def print_data(ti):
    task_instance = ti.xcom_pull(key='query_result', task_ids='select_data_task')    
    print('Resultado da consulta: ')
    for row in task_instance:
        print(row)

tsk_create_table = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag
)

tsk_insert_data = PythonOperator(
    task_id='insert_data_task',
    python_callable=insert_data,
    dag=dag
)

tsk_select_data = PythonOperator(
    task_id='select_data_task',
    python_callable=select_data,
    provide_context=True,   
    dag=dag
)

tsk_print_data = PythonOperator(
    task_id='print_data_task',
    python_callable=print_data,
    provide_context=True,
    dag=dag
)

tsk_create_table >> tsk_insert_data >> tsk_select_data >> tsk_print_data


        
