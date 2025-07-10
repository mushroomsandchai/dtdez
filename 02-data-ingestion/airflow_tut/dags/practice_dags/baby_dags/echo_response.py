from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "print_http_response",
    schedule = "@once",
    start_date = datetime(2025, 10, 7)
):
    echo = BashOperator(
        task_id = "print",
        bash_command = "curl -is https://google.com | head -n 1"
    )

    echo