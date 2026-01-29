from airflow.sdk import dag, task, get_current_context
from datetime import datetime
from load import upload_blob
from zoneinfo import ZoneInfo

def get_dates(context):
    date = context['ds'].split('-')
    return(date[1], date[0])

@dag(
    dag_id = 'week2_yellow',
    start_date = datetime(2019, 1, 1, tzinfo = ZoneInfo('America/New_York')),
    end_date = datetime(2019, 1, 31, tzinfo = ZoneInfo('America/New_York')),
    # end_date = datetime(2021, 7, 31, tzinfo = ZoneInfo('America/New_York')),
    schedule = "@monthly",
    tags = ['week2', 'hw'],
    catchup = True,
    max_active_runs = 1
)
def yellow():
    @task.bash
    def ingest():
        month, year = get_dates(get_current_context())
        return(f"""mkdir -p /tmp/week2/yellow/
        curl -Lf https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz\
         -o /tmp/week2/yellow/{year}-{month}.csv.gz
        gzip -d /tmp/week2/yellow/{year}-{month}.csv.gz
        mkdir -p /tmp/week2/green/
        curl -Lf https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz\
         -o /tmp/week2/green/{year}-{month}.csv.gz
        gzip -d /tmp/week2/green/{year}-{month}.csv.gz""")
    
    @task
    def load():
        month, year = get_dates(get_current_context())
        upload_blob("dtdez", f'/tmp/week2/yellow/{year}-{month}.csv', f'yellow/{year}-{month}.csv')
        upload_blob("dtdez", f'/tmp/week2/green/{year}-{month}.csv', f'green/{year}-{month}.csv')
    
    @task.bash
    def clean():
        print(type(get_current_context()['ds']))
        return("rm -rf /tmp/week2/")

    ingester = ingest()
    loader = load()
    cleaner = clean()

    ingester >> loader >> cleaner

yellow()