from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def do_transform(**kwargs):
    dir = kwargs['dir']
    with open(f'{dir}data.txt', 'r') as infile:
        with open(f'{dir}transformed.txt', 'w') as outfile:
            for line in infile:
                outfile.write(line.replace("Lorem", "Dummy"))

with DAG(
    dag_id = "baby_etl",
    start_date = datetime(2025, 7, 8),
    schedule = "@daily"
):

    extract = BashOperator(
        task_id = "extract",
        bash_command = f"curl -fR https://file-examples.net/wp-content/uploads/2024/02/SampleTextFile_100kb.txt -o /opt/airflow/data.txt"
    )

    transform = PythonOperator(
        task_id = "transform",
        python_callable = do_transform,
        op_kwargs = {"dir": "/opt/airflow/"}
    )

    '''
        learnt the idea of idempotence from the following task.
        before the current version of the task came about - I had "mkdir /opt/airflow/junk_files"
        which used to get triggered whenever the task ran, obv the workers threw error
        hence idempotence
        obv this baby etl is not idempotent as it doesn't account for server down time 
        of .txt provider, maybe for many more reasons that I don't yet know about.
    '''
    load = BashOperator(
        task_id = "load",
        bash_command = """
        mkdir /opt/airflow/junk_files;
        mv /opt/airflow/transformed.txt /opt/airflow/junk_files/transformed_on_{{ ds }}.txt
        rm /opt/airflow/*.txt &&
        ls -la /opt/airflow/junk_files"""
    )

    extract >> transform >> load
    