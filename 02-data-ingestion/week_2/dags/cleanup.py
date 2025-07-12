from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

@dag(
    dag_id = "cleanup",
    start_date = datetime(2025, 7, 11),
    schedule = "@daily",
)

def encompass():
    @task.bash
    def rm_parquet():
        return """echo "before removing"
        cd $AIRFLOW_HOME/ny_taxi;
        ls -la;
        rm -rf *.parquet
        rm -rf *.csv
        echo "after removing"
        ls -ls;"""

    rm_parquet()

encompass()