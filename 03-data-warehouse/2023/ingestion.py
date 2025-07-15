from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from load import upload_blob
import os

@dag(
    dag_id = "2023_week3",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2019, 12, 30),
    schedule = "@monthly",
    catchup = True,
    tags = ["homework", "2023", "fhv"]
)

def encompass():
    @task.bash(task_id = "download_file")
    def download(**kwargs):
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = execution_date.year
        month = f"{execution_date.month:02d}"
        print(year, month)
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
        return f"""mkdir -p $AIRFLOW_HOME/ny_taxi && cd $AIRFLOW_HOME/ny_taxi
        curl -fL "{url}" -o "fhv_{year}_{month}.csv.gz"
        """

    @task(task_id = "datalake")
    def datalake(**kwargs) -> int:
        execution_date = datetime.fromisoformat(kwargs['ts'])
        year = execution_date.year
        month = f"{execution_date.month:02d}"
        upload_blob(
            os.getenv('BUCKET_NAME'),
            f"/opt/airflow/ny_taxi/fhv_{year}_{month}.csv.gz",
            f"ny_taxi/fhv/{year}_{month}.csv.gz"
            )


    download() >> datalake()

encompass()