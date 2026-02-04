from google.cloud import storage


def list_buckets(bucket_name):
    """Lists all buckets."""

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name in (bucket_name, f'{bucket_name}_{storage_client.project}'):
            return(bucket.name)