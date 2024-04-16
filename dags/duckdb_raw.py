from airflow.decorators import dag, task
from airflow.models import Variable
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.utils.dates import datetime
import duckdb
import pandas as pd
from loguru import logger
import boto3
import io

bucket_name = "storage-datacraft"
geocode = 3529708
disease = 'dengue'
format = 'csv'
ew_start = 1
ew_end = 53
ey_start = 2023
ey_end = 2023
object_name = f"raw-zone/dados_dengue_{geocode}_{ew_start}_{ew_end}_{ey_start}_{ey_end}"

@dag(start_date=datetime(2024, 4, 16), schedule_interval=None, catchup=False)
def duckdb_create_raw_table():
    @task
    def read_csv_aws(bucket_name, object_name):
        try:            
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=object_name)
            csv_file = response['Body'].read().decode('utf-8')
            csv_data = pd.read_csv(io.StringIO(csv_file))
            return duckdb.from_pandas(csv_data)
        except Exception as e:
            logger.error(f"Erro ao ler arquivo do S3: {e}")
            return None

    @task
    def create_duckdb_table_from_pandas_df(csv_df):
        if csv_df is None:
            logger.error("DataFrame é None, pulando a criação da tabela.")
            return
        try:
            hook = DuckDBHook.get_hook('raw_duckdb')
            conn = hook.get_conn()
            conn.register("dengue_df", csv_df)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS raw_dengue AS 
                SELECT * FROM dengue_df;"""
            )
            linhas = conn.execute("SELECT COUNT(*) FROM raw_dengue;").fetchall()
            for linha in linhas:
                print(f"Total de {linha[0]} linhas(s)")
                
            # get the table
            return conn.execute("SELECT * FROM tb").df()
        
        except Exception as e:
            logger.error(f"Erro ao criar a tabela no DuckDB: {e}")

    csv_data = read_csv_aws(bucket_name, object_name)
    create_duckdb_table_from_pandas_df(csv_data)

duckdb_create_raw_table()
