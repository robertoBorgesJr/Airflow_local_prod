from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import statistics as sts

dag = DAG(
    '18_pythonOperator',
    description='DAG de exemplo com PythonOperator',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_view='graph')

def data_cleaner():
    # faz a leitura do arquivo CSV e faz a limpeza e correção dos dados
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')

    # atribui nomes às colunas do dataset
    dataset.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio',
                       'Saldo', 'Produtos', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu']
    
    # calcula a mediana para substituir os valores nulos na coluna Salario
    mediana = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(mediana, inplace=True)

    # substitui os valores nulos na coluna Genero pela moda (Masculino)
    dataset['Genero'].fillna('Masculino', inplace=True)
    
    # substitui os valores inválidos na coluna Idade pela mediana
    mediana = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade']<0) | (dataset['Idade']>120)] = mediana

    # remove registros duplicados com base na coluna Id
    dataset.drop_duplicates(subset='Id', keep='first', inplace=True)

    # salva o dataset limpo em um novo arquivo CSV
    dataset.to_csv("/opt/airflow/data/Churn_Clean.csv", sep=';', index=False)

tsk1 = PythonOperator(
    task_id='tsk1',
    python_callable=data_cleaner,
    dag=dag)

