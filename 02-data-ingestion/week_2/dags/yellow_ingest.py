from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from transform import establish_connection, parquet_writer
import os

@dag(
    dag_id = "yellow_ingest",
    start_date = datetime(2019, 5, 1),
    # running only for the required time period for homework
    # change start_date to datetime(2009, 1, 1) and
    #        end_date   to datetime.today() - timedelta(days = 60)
    # as ny_taxi is usually two months(60 days) late to upload latest data
    end_date = datetime(2019, 5, 30),
    schedule = "@monthly",
    catchup = True
)

def encompass():
    @task.bash(task_id = "download_file")
    def download(**kwargs):
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = execution_date.year
        month = f"{execution_date.month:02d}"
        print(year, month)
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
        return f"""mkdir -p $AIRFLOW_HOME/ny_taxi && cd $AIRFLOW_HOME/ny_taxi
        curl -f "{url}" -o "yellow_{year}_{month}.parquet"
        """

    @task(task_id = "connect_to_database")
    def connect() -> str:
        username = os.getenv('PG_USERNAME') 
        passw = os.getenv('PG_PASS') 
        host = os.getenv('PG_HOST') 
        port = os.getenv('PG_PORT') 
        table = os.getenv('PG_TABLE')
        server = f'postgresql+psycopg2://{username}:{passw}@{host}:{port}/{table}'
        connection = establish_connection(server)
        return (server)

    @task(task_id = "load_to_database")
    def load(server, **kwargs):
        execution_date = datetime.fromisoformat(kwargs['ts'])
        file_name = f'yellow_{execution_date.year}_{execution_date.month:02d}.parquet'
        print(file_name)
        parquet_writer(server, file_name)

    download() >> load(connect())

dag = encompass()