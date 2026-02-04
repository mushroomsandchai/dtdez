from google.cloud import storage

def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the standard storage
    class
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    try:
        new_bucket = storage_client.create_bucket(bucket, location="us")
    except:
        bucket_name = f'{bucket_name}_{storage_client.project}'
        bucket = storage_client.bucket(bucket_name)
        new_bucket = storage_client.create_bucket(bucket, location="us")


    print(f"Created bucket {new_bucket.name}.")
    return new_bucket.name

