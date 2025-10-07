from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  
from airflow.utils.task_group import TaskGroup  
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import logging

# coleta a taxa SELIC do Banco Central e salva em um arquivo JSON
def coleta_selic():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=18/09/2020"
    response = requests.get(url)
    dados = response.json()
    with open('/opt/airflow/data/selic.json', 'w') as f:
        json.dump(dados, f)

# coleta a taxa IPCA do Banco Central e salva em um arquivo JSON
def coleta_ipca():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial=18/09/2020"
    response = requests.get(url)
    dados = response.json()
    with open('/opt/airflow/data/ipca.json', 'w') as f:
        json.dump(dados, f)        

# coleta a taxa de câmbio US$ do Banco Central e salva em um arquivo JSON
def coleta_cambio():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json&dataInicial=18/09/2020"
    response = requests.get(url)
    dados = response.json()
    with open('/opt/airflow/data/cambio.json', 'w') as f:
        json.dump(dados, f)        

# validação dos dados coletados
def valida_dados():
    def limpa_json(path):
        df = pd.read_json(path)
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df.dropna(inplace=True)
        return df

    df_selic = limpa_json('/opt/airflow/data/selic.json')
    df_ipca = limpa_json('/opt/airflow/data/ipca.json')
    df_cambio = limpa_json('/opt/airflow/data/cambio.json')

    df_selic.to_csv('/opt/airflow/data/selic.csv', sep=';', index=False)
    df_ipca.to_csv('/opt/airflow/data/ipca.csv', sep=';', index=False)
    df_cambio.to_csv('/opt/airflow/data/cambio.csv', sep=';', index=False)

import plotly.express as px
def gerar_grafico(df, nome_tabela):
    fig = px.line(df, x='data', y='valor', title=f'{nome_tabela.upper()} - Últimos Valores') 
    return fig.to_html(full_html=False, include_plotlyjs='cdn')     

# carga dos dados no banco Postgres
def carga_dados(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    relatorio = []
    grafico = []

    def carrega_csv_em_tabela(caminho_csv, nome_tabela):
        df = pd.read_csv(caminho_csv, sep=';')

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

            # resultado = cursor.statusmessage
            # if resultado is None:
            #     ignorados += 1
            # elif 'INSERT' in resultado:
            #     inseridos += 1
            # elif 'UPDATE' in resultado:
            #     atualizados += 1

        logging.info(f'Tabela {nome_tabela}: {inseridos} registros inseridos.')
        logging.info(f'Tabela {nome_tabela}: {atualizados} registros atualizados.')
        logging.info(f'Tabela {nome_tabela}: {ignorados} registros ignorados (sem alteração).')

        relatorio.append(f"""
                         <h3>Tabela {nome_tabela.upper()}</h3>
                         <ul>
                           <li>{inseridos} registros inseridos</li>
                           <li>{atualizados} registros atualizados</li>
                           <li>{ignorados} registros ignorados (sem alteração)</li>
                         </ul>
                            """)
                         
        grafico.append(gerar_grafico(df, nome_tabela))                            
                         
    carrega_csv_em_tabela('/opt/airflow/data/selic.csv', 'selic')
    carrega_csv_em_tabela('/opt/airflow/data/ipca.csv', 'ipca')
    carrega_csv_em_tabela('/opt/airflow/data/cambio.csv', 'cambio')

    conn.commit()
    cursor.close()
    conn.close()

    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    caminho_relatorio = f'/opt/airflow/data/relatorio_carga_{timestamp}.html'
    with open(caminho_relatorio, 'w', encoding='utf-8') as f:
        f.write('<html><body>')
        f.write('<h2>Relatório de Carga de Dados - Banco Central</h2>')
        f.write(''.join(relatorio))
        f.write('</body></html>')

    caminho_relatorio_com_grafico = f'/opt/airflow/data/relatorio_carga_grafico_{timestamp}.html'
    with open(caminho_relatorio_com_grafico, 'w', encoding='utf-8') as f:
        conteudo_html = f"""
                <html>
                <head><title>Relatório Financeiro - {timestamp}</title></head>
                <body>
                <h2>Resumo da Carga de Indicadores Econômicos</h2>
                {''.join(relatorio)}
                <h3>Gráficos</h3>
                {''.join(grafico)}
                <p>Gerado em: {timestamp}</p>
                </body>
                </html>
                """ 
        f.write(conteudo_html)

    logging.info(f'Relatório de carga gerado em {caminho_relatorio}')

# outra opção para carga de dados
# esta opção não vai ser usada, mas fica aqui como referência para uso em cargas de grandes volumes
def carga_dados_copy():
    import psycopg2
    conn = psycopg2.connect("dbname='airflow' user='airflow' host='postgres' password='airflow'")
    cursor = conn.cursor()
    with open('/opt/airflow/data/selic.csv', 'r') as f:
        cursor.copy_expert("COPY selic(data, valor) FROM STDIN WITH CSV HEADER DELIMITER AS ';'", f)
    conn.commit()
    cursor.close()
    conn.close()

# definição do DAG
dag = DAG(
    'coleta_dados_bcb',
    description='Coleta as taxas SELIC, IPCA e taxa de câmbio US$ do Banco Central e salva no Postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    default_view='graph',
    tags=['produção', 'bcb', 'selic', 'IPCA', 'câmbio', 'coleta'],    
)

tsk_group_coleta = TaskGroup("coleta_dados_bcb", dag=dag)

tsk_coleta_selic = PythonOperator(
    task_id='coleta_selic',
    python_callable=coleta_selic,
    dag=dag,
    task_group=tsk_group_coleta
)

tsk_coleta_ipca = PythonOperator(
    task_id='coleta_ipca',
    python_callable=coleta_ipca,
    dag=dag,
    task_group=tsk_group_coleta
)

tsk_coleta_cambio = PythonOperator(
    task_id='coleta_cambio',
    python_callable=coleta_cambio,
    dag=dag,
    task_group=tsk_group_coleta
)

tsk_valida_dados = PythonOperator(
    task_id='valida_dados',
    python_callable=valida_dados,
    dag=dag
)

tsk_carga_dados = PythonOperator(
    task_id='carga_dados',
    python_callable=carga_dados,
    dag=dag
)

tsk_group_coleta >> tsk_valida_dados >> tsk_carga_dados
