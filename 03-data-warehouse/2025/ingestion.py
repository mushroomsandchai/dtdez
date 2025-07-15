from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from load import upload_blob
import os

@dag(
    dag_id = "week3_2025_homework",
    start_date = datetime(2024, 1, 1),
    end_date = datetime(2024, 6, 30),
    schedule = "@monthly",
    catchup = True,
    tags = ["homework", "2025", "yellow"]
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

    @task(task_id = "datalake")
    def datalake(**kwargs) -> int:
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = execution_date.year
        month = f"{execution_date.month:02d}"
        upload_blob(
            os.getenv('BUCKET_NAME'),
            f"/opt/airflow/ny_taxi/yellow_{year}_{month}.parquet",
            f"ny_taxi/yellow/{year}_{month}.parquet"
            )


    download() >> datalake()

encompass()