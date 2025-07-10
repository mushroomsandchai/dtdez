from datetime import datetime
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

@dag(
    dag_id = "filter",
    start_date = datetime(2025, 7, 10),
    schedule = "@once"
)

def encompass():

    @task.bash
    def download() -> int:
        return """
        curl -fs https://www.stats.govt.nz/assets/Uploads/Alcohol\
-available-for-consumption/Alcohol-available-for-consumptio\
n-Year-ended-December-2024/Download-data/alcohol-available-fo\
r-consumption-year-ended-december-2024.csv -o /opt/airflow/junk_files/alcohol.csv
        """

    @task
    def filter() -> pd.DataFrame:
        data = pd.read_csv('/opt/airflow/junk_files/alcohol.csv')
        data = data.loc[data['Data_value'] >= 130.0 ]
        only_beer = data.loc[data['Series_title_1'] == "Beer"]
        data.to_csv(path_or_buf = '/opt/airflow/junk_files/filtered_alcohol.csv')
        return only_beer

    @task
    def beers(only_beer: pd.DataFrame) -> int:
        only_beer.to_csv('/opt/airflow/junk_files/only_beer.csv')


    @task.bash
    def list_files() -> int:
        return "ls -la /opt/airflow/junk_files"

    download() >> beers(filter()) >> list_files()

encompass()