from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  
from airflow.utils.task_group import TaskGroup  
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import logging

def coleta_indicador(indicador, path, data_inicial, **kwargs):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{indicador}/dados?formato=json&dataInicial={data_inicial}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta um erro para códigos de status HTTP ruins
        dados = response.json()
        df = pd.DataFrame(dados)
        df.rename(columns={'data': 'data', 'valor': 'valor'}, inplace=True)
        df.to_csv(f'/opt/airflow/data/{path}.csv', sep=';', index=False)
        logging.info(f"Dados do indicador {indicador} coletados e salvos em {path}.csv")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao coletar dados do indicador {indicador}: {e}")
        raise

def valida_indicador(nome_arquivo, indicador, **kwargs):
    caminho = f'/opt/airflow/data/{nome_arquivo}.csv'
    df = pd.read_csv(caminho, sep=';')
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    df.dropna(inplace=True)

    if indicador == 'selic':
        valido = df['valor'].between(0, 20).all()
    elif indicador == 'ipca':
        valido = (df['valor'] >= 0).all()
    elif indicador == 'cambio':
        valido = df['valor'].between(3.5, 7.0).all()
    else:
        raise ValueError(f"Indicador desconhecido: {indicador}")
    
    logging.info(f"[{indicador.upper()}] Validação dos dados: {'Válido' if valido else 'Inválido'}")

    return f'carga_{nome_arquivo}' if valido else f'erro_{nome_arquivo}'

def carga_indicador(nome_arquivo, nome_tabela, **kwargs):
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    relatorio = []
    grafico = []

    df = pd.read_csv(f'/opt/airflow/data/{nome_arquivo}.csv', sep=';')
    df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.date
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    df.dropna(inplace=True)

    inseridos = 0
    atualizados = 0
    ignorados = 0

    for _, row in df.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {nome_tabela} (data, valor) 
                    VALUES (%s, %s) 
            ON CONFLICT (data) 
                DO UPDATE SET valor = EXCLUDED.valor
            RETURNING xmax;
            """,
        (row['data'], row['valor'])
        )   

        resultado = cursor.fetchone()
        if resultado is None:
            ignorados += 1
            #logging.warning(f'Tabela {nome_tabela}: registro com data {row["data"]} não foi inserido ou atualizado.')
        elif resultado[0] == 0:
            inseridos += 1
        else:
            atualizados += 1

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f'Tabela {nome_tabela}: {inseridos} registros inseridos.')

    kwargs['ti'].xcom_push(key=f'{nome_arquivo}_status', value="ok")
                        

def carga_selic(**kwargs):
    carga_indicador('selic', 'selic', **kwargs)
def carga_ipca(**kwargs):
    carga_indicador('ipca', 'ipca', **kwargs)
def carga_cambio(**kwargs):
    carga_indicador('cambio', 'cambio', **kwargs)        


# definição da DAG
dag = DAG(
    'coleta_dados_bcb_2',
    description='Coleta as taxas SELIC, IPCA e taxa de câmbio US$ do Banco Central e salva no Postgres - versão 2',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_view='graph',
    tags=['produção', 'bcb', 'selic', 'IPCA', 'câmbio', 'coleta'],    
)


# branching para validação dos dados
tsk_valida_selic = BranchPythonOperator(
    task_id='valida_selic', 
    python_callable=valida_indicador,
    op_kwargs={'nome_arquivo': 'selic', 'indicador': 'selic'},
    dag=dag
)
tsk_valida_ipca = BranchPythonOperator(
    task_id='valida_ipca',
    python_callable=valida_indicador,
    op_kwargs={'nome_arquivo': 'ipca', 'indicador': 'ipca'},
    dag=dag
)
tsk_valida_cambio = BranchPythonOperator(
    task_id='valida_cambio',
    python_callable=valida_indicador,
    op_kwargs={'nome_arquivo': 'cambio', 'indicador': 'cambio'},
    dag=dag
)

# dummies para descartar dados inválidos
tsk_erro_selic = DummyOperator(task_id='erro_selic', dag=dag)
tsk_erro_ipca = DummyOperator(task_id='erro_ipca', dag=dag)
tsk_erro_cambio = DummyOperator(task_id='erro_cambio', dag=dag)

# tasks de carga dos dados
tsk_carga_selic = PythonOperator(task_id='carga_selic', python_callable=carga_selic, dag=dag)
tsk_carga_ipca = PythonOperator(task_id='carga_ipca', python_callable=carga_ipca, dag=dag)
tsk_carga_cambio = PythonOperator(task_id='carga_cambio', python_callable=carga_cambio, dag=dag)


tsk_coleta_selic = PythonOperator(
    task_id='coleta_selic',
    python_callable=coleta_indicador,
    op_kwargs={'indicador': 11, 'path': 'selic', 'data_inicial': '01/10/2020'},
    dag=dag
)

tsk_coleta_ipca = PythonOperator(
    task_id='coleta_ipca',
    python_callable=coleta_indicador,
    op_kwargs={'indicador': 433, 'path': 'ipca', 'data_inicial': '01/10/2020'},
    dag=dag
)

tsk_coleta_cambio = PythonOperator(
    task_id='coleta_cambio',
    python_callable=coleta_indicador,
    op_kwargs={'indicador': 1, 'path': 'cambio', 'data_inicial': '01/10/2020'},
    dag=dag
)

tsk_coleta_selic >> tsk_valida_selic >> [tsk_carga_selic, tsk_erro_selic]    
tsk_coleta_ipca >> tsk_valida_ipca >> [tsk_carga_ipca, tsk_erro_ipca]    
tsk_coleta_cambio >> tsk_valida_cambio >> [tsk_carga_cambio, tsk_erro_cambio]
    