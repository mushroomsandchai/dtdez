from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from transform import establish_connection, parquet_writer
from load import upload_blob
import os

@dag(
    dag_id = "fhv_local_ingestion",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2019, 1, 1),
    schedule = "@monthly",
    catchup = True
)

def encompass():
    @task.bash(task_id = "download_fhv_data")
    def download(**kwargs):
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = f"{execution_date.year}"
        month = f"{execution_date.month:02d}"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"
        return f"""mkdir -p $AIRFLOW_HOME/ny_taxi && cd $AIRFLOW_HOME/ny_taxi
        curl -f "{url}" -o "fhv_{year}_{month}.parquet"
        """

    @task(task_id = "datalake")
    def datalake(**kwargs) -> int:
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = execution_date.year
        month = f"{execution_date.month:02d}"
        upload_blob(
            os.getenv('BUCKET_NAME'),
            f"/opt/airflow/ny_taxi/fhv_{year}_{month}.parquet",
            f"ny_taxi/fhv/{year}_{month}.parquet"
            )

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

    @task(task_id = "load")
    def load(server: str, **kwargs) -> int:
        os.listdir("ny_taxi/")
        execution_date = datetime.fromisoformat(kwargs['ts'])
        month, year = execution_date.month, execution_date.year
        filename = f'fhv_{year}_{month:02d}.parquet'
        print(f'writing {filename} to database')
        parquet_writer(server, filename)

    download() >> datalake() >> load(connect())

encompass()