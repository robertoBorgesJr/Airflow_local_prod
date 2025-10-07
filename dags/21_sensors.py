from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests

# primeiramente é necessário cadastrar uma conexão do tipo HTTP no Airflow com o ID 'connection'
# foi cadastrada a conexão com nome 'Fixer' e o endereço base 'https://data.fixer.io/api/'

dag = DAG(
    '21_sensors', 
    description='Exemplo de sensor HTTP',
    schedule_interval=None, 
    start_date=datetime(2025,8,28),
    catchup=False,
    default_view='graph'    
)

def query_api():
    response = requests.get("https://data.fixer.io/api/latest") # é necessário informar o endereço completo, inclusive com o endpoint "latest"
    print(response.text)

tsk_api = HttpSensor(
    task_id='tsk_api',
    http_conn_id='Fixer',  # ID da conexão HTTP cadastrada no Airflow    
    endpoint='latest',     # endpoint a ser verificado
    poke_interval=30,      # intervalo de verificação em segundos
    timeout=120,           # tempo máximo de espera em segundos
    dag=dag
)

tsk_http = PythonOperator(
    task_id='tsk_http',
    python_callable=query_api,
    dag=dag
)

tsk_api >> tsk_http