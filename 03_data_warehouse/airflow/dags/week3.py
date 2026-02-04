from airflow.sdk import task, dag, get_current_context
from datetime import datetime

BUCKET = 'homework'
DATASET = 'homework'
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
WORK_DIR = '/tmp/week3'

@dag(
    start_date = datetime(2024, 1, 1),
    end_date = datetime(2024, 6, 30),
    dag_id = 'week3',
    tags = ['hw', 'week3', 'create_bucket', 'bucket_exists'],
    catchup = True,
    schedule = '@monthly',
    max_active_runs = 1
)
def week3():
    @task
    def get_date():
        logical_date = get_current_context()['logical_date']
        return({
                'year': str(logical_date.year),
                'month': str(logical_date.month).zfill(2)
                })
    
    @task.bash
    def ingest(date):
        month, year = date['month'], date['year']
        return(f"""
                mkdir -p {WORK_DIR}
                curl -Lf {BASE_URL}{year}-{month}.parquet -o {WORK_DIR}/{year}-{month}.parquet
        """)

    @task
    def bucket_check():
        from helpers.bucket_checker import list_buckets
        bucket = list_buckets(BUCKET)

        if not bucket:
            print('Bucket does not exist. Attempting bucket creation....')
            from helpers.create_bucket import create_bucket_class_location
            return(create_bucket_class_location(BUCKET))
        return(bucket)


    @task(
        trigger_rule = 'all_success'
    )
    def load(date, bucket_name):
        month, year = date['month'], date['year']
        from helpers.load import upload_blob
        upload_blob(bucket_name, f'{WORK_DIR}/{year}-{month}.parquet', f'week3/{year}-{month}.parquet')


    @task.bash(
        trigger_rule = 'all_done'
    )
    def clean():
        return(f"rm -rf {WORK_DIR}")

    date = get_date()
    ingester = ingest(date)
    checker = bucket_check()
    loader = load(date, checker)
    cleaner = clean()

    ingester >> checker >> loader >> cleaner

week3()