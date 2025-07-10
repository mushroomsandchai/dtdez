from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "print_http_response",
    schedule = "@daily",
    start_date = datetime(2025, 7, 10),
    catchup = False
):
    echo_bash = BashOperator(
        task_id = "echo_bash",
        bash_command = "curl -is https://google.com | head -n 1"
    )

    echo_bash