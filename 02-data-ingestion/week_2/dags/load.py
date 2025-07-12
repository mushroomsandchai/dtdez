from google.cloud import storage


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