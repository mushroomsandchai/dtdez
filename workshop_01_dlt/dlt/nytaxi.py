import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

def fetch_page(page: int):
    response = requests.get(
        BASE_URL,
        params={"page": page, "page_size": 1000},
    )
    response.raise_for_status()
    return response.json()

@dlt.resource(
    write_disposition = "append"
)
def nytaxi():
    page = 1

    while True:
        data = fetch_page(page)
        if not data:
            return
        yield data
        page += 1


pipeline = dlt.pipeline(
    pipeline_name = "nytaxi",
    destination = "duckdb",
    dataset_name = "dlt",
    progress = "log"
)

if __name__ == "__main__":
    load_info = pipeline.run(nytaxi())
    print(load_info)