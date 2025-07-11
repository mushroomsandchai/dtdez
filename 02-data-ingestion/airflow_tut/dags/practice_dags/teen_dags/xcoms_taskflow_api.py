from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
@dag(
    dag_id = "good_morning_decorators",
    start_date = datetime(2025, 7, 10),
    schedule = "@hourly",
    catchup = True
)

def encompass():
    @task.bash
    def echo_bash() -> int:
        return 'echo "good morning, it is currently {{ ts }}"'

    @task
    def print_python(**kwargs) -> int:
        ts = kwargs['ts']
        print(f"good morning, it's {ts}")
        return 46

    @task
    def print_xcom_pull(com: int) -> int:
        print(f'i should be passed 46 as an integer, it\'s hardcoded\ncom: {com}')
        return 0

    echo_bash() >> print_xcom_pull(print_python())

encompass()