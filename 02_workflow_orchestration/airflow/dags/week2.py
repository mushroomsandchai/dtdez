from airflow.sdk import dag, task, get_current_context
from datetime import datetime
from load import upload_blob
from zoneinfo import ZoneInfo

WORK_DIR = '/tmp/week2/'
DATASET = 'homework'
BUCKET_NAME = 'homework_dtdez'

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

    @task
    def get_dates(logical_date = None):
        return({
                'month': str(logical_date.month).zfill(2), 
                'year': logical_date.year
                })

    @task.bash
    def ingest(date):
        context = get_current_context()
        month, year = date['month'], date['year']
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
    def load(date):
        context = get_current_context()
        month, year = date['month'], date['year']
        if year == 2020 and month == "12":
            import os
            print(os.path.getsize(f"{WORK_DIR}yellow/{year}-{month}.csv") / (1000000))
        upload_blob(BUCKET_NAME, f'{WORK_DIR}yellow/{year}-{month}.csv', f'yellow/{year}-{month}.csv')
        upload_blob(BUCKET_NAME, f'{WORK_DIR}green/{year}-{month}.csv', f'green/{year}-{month}.csv')

    @task(
        trigger_rule = 'all_success'
    )
    def big_query_insert(date):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook()
        client = hook.get_client()
        month, year = date['month'], date['year']
        for taxi in (['yellow', 'green']):
            green_query = f"""
                                create table if not exists {DATASET}.{taxi}_{year} (
                                    VendorID INT64,
                                    lpep_pickup_datetime TIMESTAMP,
                                    lpep_dropoff_datetime TIMESTAMP,
                                    store_and_fwd_flag BOOL,
                                    RatecodeID INT64,
                                    PULocationID INT64,
                                    DOLocationID INT64,
                                    passenger_count INT64,
                                    trip_distance FLOAT64,
                                    fare_amount FLOAT64,
                                    extra FLOAT64,
                                    mta_tax FLOAT64,
                                    tip_amount FLOAT64,
                                    tolls_amount FLOAT64,
                                    ehail_fee STRING,
                                    improvement_surcharge FLOAT64,
                                    total_amount FLOAT64,
                                    payment_type INT64,
                                    trip_type INT64,
                                    congestion_surcharge FLOAT64
                                );

                                create or replace external table {DATASET}.{taxi}_{year}_{month}_ext options (
                                uris = ['gs://{BUCKET_NAME}/{taxi}/{year}-{month}.csv'],
                                format = 'csv'
                                );

                                insert into {DATASET}.{taxi}_{year}
                                select * from {DATASET}.{taxi}_{year}_{month}_ext;

                                drop table {DATASET}.{taxi}_{year}_{month}_ext;
                            """

            yellow_query = f"""create table if not exists {DATASET}.{taxi}_{year} (
                                    VendorID INT64,
                                    tpep_pickup_datetime TIMESTAMP,
                                    tpep_dropoff_datetime TIMESTAMP,
                                    passenger_count INT64,
                                    trip_distance FLOAT64,
                                    RatecodeID INT64,
                                    store_and_fwd_flag BOOL,
                                    PULocationID INT64,
                                    DOLocationID INT64,
                                    payment_type INT64,
                                    fare_amount FLOAT64,
                                    extra FLOAT64,
                                    mta_tax FLOAT64,
                                    tip_amount FLOAT64,
                                    tolls_amount FLOAT64,
                                    improvement_surcharge FLOAT64,
                                    total_amount FLOAT64,
                                    congestion_surcharge FLOAT64
                                );

                                create or replace external table {DATASET}.{taxi}_{year}_{month}_ext options (
                                uris = ['gs://{BUCKET_NAME}/{taxi}/{year}-{month}.csv'],
                                format = 'csv'
                                );

                                insert into {DATASET}.{taxi}_{year}
                                select * from {DATASET}.{taxi}_{year}_{month}_ext;

                                drop table {DATASET}.{taxi}_{year}_{month}_ext;
                             """
            if taxi == 'yellow':
                if year == '2020' or (year == '2021' and month == '03'):
                    query_job = client.query(yellow_query)
            elif year == '2020':
                query_job = client.query(green_query)

    @task.bash(trigger_rule = "all_done")
    def clean():
        return(f"rm -rf {WORK_DIR}")

    date = get_dates()
    ingester = ingest(date)
    loader = load(date)
    inserter = big_query_insert(date)
    cleaner = clean()

    ingester >> loader >> inserter >> cleaner

week_2_ingestion()