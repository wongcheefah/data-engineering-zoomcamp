import os
import dlt
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# generator for loading trip data by year and service
def load_csv(year, service):
    if service == "fhv":

        taxi_dtypes = {
            "dispatching_base_num": str,
            "PUlocationID": pd.Int64Dtype(),
            "DOlocationID": pd.Int64Dtype(),
            "SR_Flag": pd.Int64Dtype(),
            "Affiliated_base_number": str,
        }
    else:
        taxi_dtypes = {
            "VendorID": pd.Int64Dtype(),
            "passenger_count": pd.Int64Dtype(),
            "trip_distance": float,
            "RatecodeID": pd.Int64Dtype(),
            "store_and_fwd_flag": str,
            "PULocationID": pd.Int64Dtype(),
            "DOLocationID": pd.Int64Dtype(),
            "payment_type": pd.Int64Dtype(),
            "fare_amount": float,
            "extra": float,
            "mta_tax": float,
            "tip_amount": float,
            "tolls_amount": float,
            "improvement_surcharge": float,
            "total_amount": float,
            "congestion_surcharge": float,
            "trip_type": pd.Int64Dtype(),
        }

    if service == "green":
        parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
    elif service == "yellow":
        parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    else:
        parse_dates = ["pickup_datetime", "dropOff_datetime"]

    for month in range(12):

        filename = f"{service}_tripdata_{year}-{month+1:02d}.csv.gz"
        url = f"{init_url}{service}/{filename}"

        for row in pd.read_csv(
            url,
            sep=",",
            compression="gzip",
            dtype=taxi_dtypes,
            parse_dates=parse_dates,
            iterator=True,
        ):
            yield row


def web_to_bq(year, service):
    # define the connection to load to
    generators_pipeline = dlt.pipeline(destination="bigquery", dataset_name=dataset)

    # load and append the data by year and service into tables named by service
    info = generators_pipeline.run(
        load_csv(year, service),
        table_name=f"{service}_tripdata",
        write_disposition="append",
    )

    # inspect the load outcome
    print(info)

    # create a BigQuery client object
    client = bigquery.Client(credentials=creds, project=project_id)

    query = """
        SELECT *
        FROM f`{dataset}.{table_name}`
    """

    client.query(query)  # Make an API request.


creds = service_account.Credentials.from_service_account_file(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

dataset = "ny_taxi_ride_data"

init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

service = "green"
for year in [2019, 2020]:
    print(f"\n{service} - {year}")
    web_to_bq(year, service)

service = "yellow"
for year in [2019, 2020]:
    print(f"\n{service} - {year}")
    web_to_bq(year, service)

service = "fhv"
year = 2019
print(f"\n{service} - {year}")
web_to_bq(year, service)
