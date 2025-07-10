from datetime import datetime
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

@dag(
    dag_id = "csv_to_parquet",
    start_date = datetime(2025, 7, 8),
    schedule = "@once",
    catchup = False
)

def encompass():
    
    @task.bash
    def download() -> int:
        return """
                mkdir /opt/airflow/junk_files;
                curl -fs https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/\
Annual-enterprise-survey-2024-financial-year-provisional/Download-data/annual\
-enterprise-survey-2024-financial-year-provisional-size-bands.csv -o /opt/airfl\
ow/junk_files/data.csv
                """

    @task
    def convert() -> int:
        data = pd.read_csv('/opt/airflow/junk_files/data.csv')
        data.to_parquet(path = '/opt/airflow/junk_files/data.parquet')

    download() >> convert()

encompass()