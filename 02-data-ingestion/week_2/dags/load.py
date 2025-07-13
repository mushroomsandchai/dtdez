from google.cloud import storage, bigquery


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # destination_blob_name = "path where the file should be stored?"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # todo
    # generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, 
                            # if_generation_match=generation_match_precondition
                            )

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def upload_dataset(project_id, file_name, file_path):
    client = bigquery.Client()
    # project_id is actually used to crosscheck existing project
    # file name to be passed without extension to yield a table of the same name
    # without extension to comply with sql best practices
    # ny_taxi will the dataset in bigquery corresponding to the project id
    # file to be passed in parquet format
    table_id = f"{project_id}.ny_taxi.{file_name.rsplit(".", 1)[0]}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result() 

    print(f"Loaded {load_job.output_rows} rows into {table_id}")
