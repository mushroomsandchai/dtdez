from datetime import datetime
from random import randint

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "remove_baby_dag_files",
    schedule = "@daily",
    start_date = datetime(2025, 7, 10)
):

    remove_files = BashOperator(
        task_id = "remove_baby_dag_files",
        bash_command = '''
        rm -rf /opt/airflow/*txt
        rm -rf /opt/airflow/junk_files
        ls -la /opt/airflow/
        '''
    )

    remove_files