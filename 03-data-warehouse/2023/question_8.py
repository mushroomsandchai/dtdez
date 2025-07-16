from datetime import datetime
import os
import pandas as pd
from airflow.decorators import dag, task
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


@dag(
    dag_id = "gz_parquet_bucket",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2019, 12, 30),
    schedule = "@monthly",
    catchup = True,
    tags = ["2023", "week3", "homework", "question 8"]
)

def encompass():
    @task.bash
    def download(**kwargs) -> int:
        date = datetime.fromisoformat(kwargs['ts'])
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{date.year}-{date.month:02d}.csv.gz"
        return f"""mkdir /opt/airflow/ny_taxi;
        mkdir /opt/airflow/ny_taxi/gz;
        cd /opt/airflow/ny_taxi/gz/
        curl -fL {url} -o "fhv_{date.year}_{date.month:02}.csv.gz"
        """

    @task.bash
    def unzip(**kwargs) -> int:
        date = datetime.fromisoformat(kwargs['ts'])
        return f"""echo "before unzipping"
        cd /opt/airflow/ny_taxi/gz/
        ls -la
        gunzip fhv_{date.year}_{date.month:02d}.csv.gz
        echo "after unzipping"
        ls -la
        """

    @task
    def convert(**kwargs) -> int:
        date = datetime.fromisoformat(kwargs['ts'])
        path = "/opt/airflow/ny_taxi/gz/"
        file = f"fhv_{date.year}_{date.month:02d}.csv"
        fhv_df = pd.read_csv(path + file)
        fhv_df.to_parquet(path + file.rstrip("csv") + "parquet")
        print(os.listdir(path))

    @task
    def load(**kwargs) -> int:
        date = datetime.fromisoformat(kwargs['ts'])
        path = "/opt/airflow/ny_taxi/gz/"
        file = f"fhv_{date.year}_{date.month:02d}.parquet"
        upload_blob("dtdez_465215_parquet_dump", path + file, f"ny_taxi/fhv/{file}")


    download() >> unzip() >> convert() >> load()

encompass()