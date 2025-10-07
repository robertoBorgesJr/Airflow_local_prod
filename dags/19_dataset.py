from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts

dataset = Dataset('/opt/airflow/data/Churn_producer.csv')

dag = DAG(
    '19_dataset',
    description='Leitura da tabela Churn_producer com atualização em tempo real',
    schedule=[dataset],
    start_date=datetime(2025, 8, 27),
    catchup=False,
    default_view='graph'
)

def data_cleaner():
   
    # faz a leitura do arquivo CSV e faz a limpeza e correção dos dados
    df = pd.read_csv("/opt/airflow/data/Churn_producer.csv", sep=';')

    # atribui nomes às colunas do dataset
    df.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio',
                       'Saldo', 'Produtos', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu']
    
    # calcula a mediana para substituir os valores nulos na coluna Salario
    df['Salario'].fillna(sts.median(df['Salario']), inplace=True)

    # substitui os valores nulos na coluna Genero pela moda (Masculino)
    df['Genero'].fillna('Masculino', inplace=True)
    
    # substitui os valores inválidos na coluna Idade pela mediana
    mediana_idade = sts.median(df['Idade'])
    df.loc[(df['Idade'] < 0) | (df['Idade'] > 120), 'Idade'] = mediana_idade

    # remove registros duplicados com base na coluna Id
    df.drop_duplicates(subset='Id', keep='first', inplace=True)

    df.to_excel('/opt/airflow/data/Churn_Clean.xlsx', index=False)
tskReadDataset = PythonOperator(
    task_id='tskReadDataset',
    python_callable=data_cleaner,
    dag=dag)
