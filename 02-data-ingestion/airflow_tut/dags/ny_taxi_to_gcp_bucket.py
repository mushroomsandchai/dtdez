from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def welcome(**kwargs):
    print(f"hello, world at {kwargs['execution_date']}")

with DAG(
    dag_id = "my_first_actual_dag",
    start_date = datetime(2024, 1, 1),
    schedule = "@monthly",
    catchup = True
):
    echo = BashOperator(
        task_id = "echo",
        bash_command = 'echo "bash just echoed something, congrats you\'ve figured out BashOperator"'
    )

    printer = PythonOperator(
        task_id = "printer",
        python_callable = welcome,
        op_kwargs = {"execution_date": "{{ ds }}"}
    )

    echo >> printer