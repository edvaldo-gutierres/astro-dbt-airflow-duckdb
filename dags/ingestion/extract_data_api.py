from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
from airflow.models import Variable
import requests
import pandas as pd
from io import StringIO
import boto3


# Configurações padrão dos argumentos que serão passados para cada tarefa
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
    'catchup': False
}

# Define a DAG
dag = DAG(
    'get_data_csv_api',
    default_args=default_args,
    description='Fetch data from API and store in DuckDB',
    schedule_interval=timedelta(days=1)
)

 # Informa parâmetros
bucket_name = "storage-datacraft"
url = 'https://info.dengue.mat.br/api/alertcity?'
geocode = 3529708
disease = 'dengue'
format = 'csv'
ew_start = 1
ew_end = 53
ey_start = 2023
ey_end = 2023
object_name = f"raw-zone/dados_dengue_{geocode}_{ew_start}_{ew_end}_{ey_start}_{ey_end}"
    

def get_csv_api_data(url: str, geocode :int, disease: str, format: str, ew_start: int, ew_end: int, ey_start: int, ey_end: int) -> None:
    """
    Função para obter os dados da API em formato CSV e retorná-los como um DataFrame pandas.
    
    Parâmetros:
    url: URL da API (str: https://info.dengue.mat.br/api/alertcity?)
    geocode: código IBGE da cidade
    disease: tipo de doença a ser consultado (str:dengue|chikungunya|zika)
    ew_start: semana epidemiológica de início da consulta (int:1-53)
    ew_end: semana epidemiológica de término da consulta (int:1-53)
    ey_start: ano de início da consulta (int:0-9999)
    ey_end: ano de término da consulta (int:0-9999)
    
    Retorna:
    pd.DataFrame: DataFrame pandas contendo os dados CSV da API.
    None: Retorna None se não for possível obter os dados da API.
    """

    # URL completa
    url_api = url + "geocode=" + str(geocode) + '&disease=' + str(disease) + '&format=' + str(format) + '&ew_start=' + str(ew_start) + '&ew_end=' + str(ew_end) + '&ey_start=' + str(ey_start) + '&ey_end=' + str(ey_end)
    print(url_api)

    # Faz a solicitação GET para a API
    response = requests.get(url_api)

    # Verifica se a solicitação foi bem-sucedida
    if response.status_code == 200:
        # Converte os dados em um dataframe
        data = StringIO(response.text)
        dataframe = pd.read_csv(data, delimiter=',')
        # Inserir coluna Ultima Atualização
        dataframe["last_update"] = datetime.now()
        # Salva dataframe em um arquivo .csv
        path = f'./dags/ingestion/data/dados_dengue_{geocode}_{ew_start}_{ew_end}_{ey_start}_{ey_end}.csv'
        dataframe.to_csv(path)
        print(f"Arquivo csv salvo em {path}")

    else:
        # Log an error and raise an exception
        error_message = f"Failed to fetch data from API. Status Code: {response.status_code}"
        print(error_message)
        raise Exception(error_message)
    
    
# Função para enviar o arquivo CSV para o S3
def csv_to_s3(path_file,bucket_name, object_name):
    # Cria o cliente S3
    s3_client = boto3.client("s3")

    # Faz o upload do arquivo para o S3
    s3_client.upload_file(
        Filename=path_file, Bucket=bucket_name, Key=object_name
    )


# Define as tarefas da DAG
get_data = PythonOperator(
    task_id='get_api_data',
    python_callable=get_csv_api_data,
    op_kwargs={
        'url': 'https://info.dengue.mat.br/api/alertcity?',
        'geocode': geocode,
        'disease': disease,
        'format': format,
        'ew_start': ew_start,
        'ew_end': ew_end,
        'ey_start': ey_start,
        'ey_end': ey_end
    },
    dag=dag
)

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=csv_to_s3,
    op_kwargs={
        'path_file': f'./dags/ingestion/data/dados_dengue_{geocode}_{ew_start}_{ew_end}_{ey_start}_{ey_end}.csv',
        'bucket_name': bucket_name, 
        'object_name': f'{object_name}.csv'
    },
    dag=dag
)


get_data >> upload_to_s3