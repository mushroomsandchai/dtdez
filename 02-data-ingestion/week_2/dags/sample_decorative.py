from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

@dag(
    dag_id = "sample_decorative",
    start_date = datetime(2025, 7, 1),
    schedule = "0 6 * * *",
    catchup = True,
    tags = ["sample", "lame"]
)

def encompass():
    @task.bash
    def echo_morning() -> int:
        return 'echo "good morning, it is {{ ts }}"'

    @task
    def print_morning(**kwargs) -> int:
        print(f"top of the morning to you, sir/madam. it is {kwargs['ts']}")
        return 43

    @task
    def print_xcom(com: int) -> int:
        print(f"i received {com}")
        return 0

    echo = echo_morning()
    num = print_morning()
    print_xcom(num)

encompass()
