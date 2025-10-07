from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# primeiramente é necessário cadastrar uma conexão com o postgres no Airflow com o ID 'postgres'

dag = DAG(
    '22_banco_de_dados', 
    description='Exemplo de conexão com banco de dados Postgres',
    schedule_interval=None, 
    start_date=datetime(2025,9,2),
    catchup=False,
    default_view='graph'    
)

def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_data')
    print('Resultado da consulta: ')
    for row in task_instance:
        print(row)

tsk_create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',  # ID da conexão com o Postgres cadastrada no Airflow
    sql='create table if not exists clientes (id serial primary key, nome varchar(50), idade int, salario float);',
    dag=dag 
)

tsk_insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',
    sql="insert into clientes (nome, idade, salario) values ('João Silva', 30, 5000.00);",
    dag=dag
)

tsk_query_data = PostgresOperator(
    task_id='query_data',
    postgres_conn_id='postgres',
    sql="select * from clientes;",
    dag=dag
)

tsk_print_result = PythonOperator(
    task_id='print_result',
    python_callable=print_result,
    provide_context=True,
    dag=dag
)

tsk_create_table >> tsk_insert_data >> tsk_query_data >> tsk_print_result

