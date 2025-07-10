from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "list_files",
    schedule = "@daily",
    start_date = datetime(2025, 7, 1),
):
    list_files = BashOperator(
        task_id = "list_home_directory",
        bash_command = "ls -la /opt/airflow/"
    )

    list_files