from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from transform import establish_connection, write_to_postgres
import os

@dag(
    dag_id = "local_etl",
    start_date = datetime(2025, 4, 20),
    # accounting for late uploads from ny_taxi
    end_date = datetime.today() - timedelta(days=60),
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
        return f"""mkdir $AIRFLOW_HOME/ny_taxi;
        curl -f "{url}" -o $AIRFLOW_HOME/ny_taxi/{year}_{month}.parquet
        ls -ls $AIRFLOW_HOME/ny_taxi/
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
        file_name = f'{execution_date.year}_{execution_date.month:02d}.parquet'
        print(file_name)
        write_to_postgres(server, file_name)

    download() >> load(connect())

encompass()