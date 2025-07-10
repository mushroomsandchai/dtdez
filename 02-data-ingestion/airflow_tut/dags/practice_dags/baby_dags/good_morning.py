from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "good_morning",
    schedule = "0 6 * * *",
    start_date = datetime(2025, 7, 1),
    catchup = True
):

    good_morning = BashOperator(
        task_id = "say_good_morning",
        bash_command = 'echo "good morning! it\'s {{ ds }}"'
    )

    good_morning