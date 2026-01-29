from airflow.sdk import dag, task, get_current_context
from datetime import datetime
from load import upload_blob
from zoneinfo import ZoneInfo

WORK_DIR = '/tmp/week2/'

def get_dates(context):
    month, year = str(context['logical_date'].month).zfill(2), context['logical_date'].year
    return (month, year)

@dag(
    dag_id = 'week2',
    start_date = datetime(2019, 1, 1, tzinfo = ZoneInfo('America/New_York')),
    end_date = datetime(2021, 7, 31, tzinfo = ZoneInfo('America/New_York')),
    schedule = "@monthly",
    tags = ['week2', 'hw'],
    catchup = True,
    max_active_runs = 1
)
def week_2_ingestion():
    @task.bash
    def ingest():
        context = get_current_context()
        month, year = get_dates(context)
        return(f"""set -euo pipefail
        mkdir -p {WORK_DIR}yellow/
        curl -Lf https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz\
         -o {WORK_DIR}yellow/{year}-{month}.csv.gz
        gzip -d {WORK_DIR}yellow/{year}-{month}.csv.gz
        mkdir -p {WORK_DIR}green/
        curl -Lf https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz\
         -o {WORK_DIR}green/{year}-{month}.csv.gz
        gzip -d {WORK_DIR}green/{year}-{month}.csv.gz""")
    
    @task
    def load():
        context = get_current_context()
        month, year = get_dates(context)
        if year == 2020 and month == "12":
            import os
            print(os.path.getsize(f"{WORK_DIR}yellow/{year}-{month}.csv") / (1000000))
        upload_blob("dtdez", f'{WORK_DIR}yellow/{year}-{month}.csv', f'yellow/{year}-{month}.csv')
        upload_blob("dtdez", f'{WORK_DIR}green/{year}-{month}.csv', f'green/{year}-{month}.csv')
    
    @task.bash(trigger_rule = "all_done")
    def clean():
        return(f"rm -rf {WORK_DIR}")

    ingester = ingest()
    loader = load()
    cleaner = clean()

    ingester >> loader >> cleaner

week_2_ingestion()