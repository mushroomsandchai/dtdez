from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
import os
from load import upload_blob
from transform import establish_connection, csv_to_parquet, parquet_writer

@dag(
    dag_id = "zone_ingest",
    start_date = datetime(2025, 7, 12),
    schedule = "@once"
)

def encompass():
    @task.bash(task_id = "download")
    def download() -> str:
        return """cd $AIRFLOW_HOME
        mkdir -p ny_taxi
        cd ny_taxi
        curl -f "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" -o "zone.csv"
        ls -la"""

    @task(task_id = "convert")
    def convert() -> int:
        csv_to_parquet('zone.csv')

    @task(task_id = "datalake")
    def datalake() -> int:
        upload_blob(os.getenv('BUCKET_NAME'), "/opt/airflow/ny_taxi/zone.csv", "ny_taxi/zone.csv")

    @task(task_id = "establish_database")
    def connect() -> str:
        username = os.getenv('PG_USERNAME')
        password = os.getenv('PG_PASS') 
        host = os.getenv('PG_HOST') 
        port = os.getenv('PG_PORT') 
        table = os.getenv('PG_TABLE') 
        server = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{table}'
        establish_connection(server)
        return (server)

    @task(task_id = "loading_to_database")
    def load(server):
        print(os.listdir())
        parquet_writer(server, 'zone.parquet')

    download() >> convert() >> datalake() >> load(connect())


encompass()