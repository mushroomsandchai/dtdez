from datetime import datetime
from random import randint

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def log(**kwargs):
    dir = kwargs['directory']
    with open(f'{dir}random_log.txt', 'w') as logfile:
        logfile.write(f'{randint(1, 6)} @ {kwargs["execution_date"]}')


with DAG(
    dag_id = "log_random_int",
    schedule = "@daily",
    catchup = True,
    start_date = datetime(2025, 7, 1)
):

    log_random = PythonOperator(
        task_id = "log_random",
        python_callable = log,
        op_kwargs = {
            "directory": "/opt/airflow/",
            "execution_date": "{{ ds }}"
            }
    )

    log_random