from airflow.sdk import task, dag, get_current_context
from datetime import datetime

DATASET = 'homework'
WORK_DIR = '/tmp/week4/'
GCP_BUCKET = 'week4_dtdez'
TAXI_TYPES = ['yellow', 'green', 'fhv']
BASE_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

@dag(
    dag_id = 'week4',
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2020, 12, 31),
    schedule = '@monthly',
    catchup = True,
    tags = ['dbt', 'week4', 'hw', 'green', 'csv.gz', 'yellow', 'fhv'],
    max_active_runs = 1
)
def week4():
    @task
    def get_dates():
        logical_date = get_current_context()['logical_date']
        return({
            'year': str(logical_date.year),
            'month': str(logical_date.month).zfill(2)
        })

    @task.bash
    def ingest(date):
        month, year = date['month'], date['year']
        taxis = " ".join(TAXI_TYPES)
        return(f"""
        mkdir -p {WORK_DIR}
        for i in {taxis}; do
            if [[ "$i" != "fhv" || {year} -eq 2019 ]]; then
                curl -Lf {BASE_URL}"$i"/"$i"_tripdata_{year}-{month}.csv.gz -o {WORK_DIR}"$i"-{year}-{month}.csv.gz
                gzip -d {WORK_DIR}"$i"-{year}-{month}.csv.gz
            fi
        done
        ls -la {WORK_DIR}
        """)

    @task
    def bucket_check():
        from helpers.bucket_checker import list_buckets
        bucket = list_buckets(GCP_BUCKET)
        if not bucket:
            print("Given bucket not found. Attempting to create a new bucket.")
            from helpers.create_bucket import create_bucket_class_location
            bucket = create_bucket_class_location(GCP_BUCKET)
        return(bucket)
    
    @task(trigger_rule = 'all_success')
    def load(date, bucket):
        month, year = date['month'], date['year']
        from helpers.load import upload_blob
        for taxi in TAXI_TYPES:
            if taxi != 'fhv' or year == "2019":
                upload_blob(bucket, f'{WORK_DIR}{taxi}-{year}-{month}.csv', f'{taxi}/{year}-{month}.csv')

    @task
    def bq_load(date, bucket):
        month, year = date['month'], date['year']
        if month == "12":
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            hook = BigQueryHook()
            client = hook.get_client()

            if year == "2019":
                print("creating external fhv table")
                query = client.query(f"""create or replace external table {DATASET}.ext_fhv options(
                    uris = ['gs://{bucket}/fhv/2019*.csv'],
                    format = 'csv'
                );""")
                print(query)
            else:
                for taxi in ('yellow', 'green'):
                    print(f"creating external {taxi} table")
                    query = client.query(f"""create or replace external table {DATASET}.ext_{taxi} options(
                    uris = ['gs://{bucket}/{taxi}/2019*.csv', 'gs://{bucket}/{taxi}/2020*.csv'],
                    format = 'csv'
                    );""")
                    print(query)

    @task.bash(trigger_rule = 'all_done')
    def clean():
        return(f'rm -rf {WORK_DIR} && ls -la /tmp/')

    date = get_dates()
    ingester = ingest(date)
    bucket = bucket_check()
    loader = load(date, bucket)
    bq_loader = bq_load(date, bucket)
    cleaner = clean()

    ingester >> bucket >> loader >> bq_loader >> cleaner


week4()