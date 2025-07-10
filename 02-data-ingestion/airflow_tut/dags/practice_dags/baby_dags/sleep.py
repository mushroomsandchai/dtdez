from datetime import datetime
import time

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def sleepy(**kwargs):
    print(f"sleeping for {kwargs['time']}")
    time.sleep(kwargs['time'])
    print(f"woke up")

with DAG(
    dag_id = "sleepy",
    schedule = "@hourly",
    start_date = datetime(2025, 7, 9)
):

    sleepyfor2 = PythonOperator(
        task_id = "sleepyfor2",
        python_callable = sleepy,
        op_kwargs = {"time": 2}
    )

    sleepyfor5 = PythonOperator(
        task_id = "sleepyfor5",
        python_callable = sleepy,
        op_kwargs = {"time": 5}
    )

    sleepyfor2 >> sleepyfor5