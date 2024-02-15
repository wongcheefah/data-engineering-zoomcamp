## Module 3 Homework

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>Loading taxi trip data onto GCS using Mage:</b>

Create dataset in BigQuery.
```
bq --location=asia-southeast1 mk -d pristine-sphere-412713:ny_taxi
```

Start Mage from [02-orchestration-mage](../02-orchestration-mage/) and create a new batch pipeline. 
<i>(Copy of loader and exporter scripts and metadata YAML downloaded into data_loaders, data_exporters and pipelines directories)</i>
- Set global variables `fleet` and  `year`
  - fleet : 'green'
  - year : 2022

- Add a data loader (Data loader > Python > API)
```
import io
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    monthly_dfs = []

    fleet = kwargs['fleet']
    year = kwargs['year']

    for month in range(1, 13):
        filename = f'{fleet}_tripdata_{year}-{month:02d}.parquet'
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
        print(f'Loading {fleet} fleet {year}-{month} trip data from {url}')
        response = requests.get(url)

        df_by_month = pq.read_table(io.BytesIO(response.content)).to_pandas()
        monthly_dfs.append(df_by_month)

    return pd.concat(monthly_dfs, ignore_index=True)
```

- Add a data exporter (Data exporter > Python > Google Cloud Storage)
```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    fleet = kwargs['fleet']
    year = kwargs['year']
    bucket_name = 'mage-zoomcamp-wcf'
    object_key = f'{fleet}/{fleet}_tripdata_{year}.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
```

No change to `io_config.yaml` and GCS bucket name from module 2. Create 

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
```
CREATE OR REPLACE EXTERNAL TABLE
  pristine-sphere-412713.ny_taxi.external_green_tripdata
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-wcf/green/green_tripdata_2022.parquet']
);
```

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
```
CREATE OR REPLACE TABLE
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned AS
SELECT
  VendorID,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime/1000 AS INT64)) AS lpep_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime/1000 AS INT64)) AS lpep_dropoff_datetime,
  store_and_fwd_flag,
  CAST(RatecodeID AS INT64) AS RatecodeID,
  PULocationID,
  DOLocationID,
  CAST(passenger_count AS INT64) AS passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  CAST(payment_type AS INT64) AS payment_type,
  CAST(trip_type AS INT64) AS trip_type,
  congestion_surcharge
FROM
  pristine-sphere-412713.ny_taxi.external_green_tripdata;
```
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

```
SELECT
  COUNT(*)
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned;
```

Ans: 840,402


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

```
SELECT
  COUNT(DISTINCT PULocationID)
FROM 
  pristine-sphere-412713.ny_taxi.external_green_tripdata;
```
```
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned;
```

Ans: 0 MB for the External Table and 6.41MB for the Materialized Table


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

```
SELECT
  COUNT(*)
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned
WHERE
  fare_amount = 0;
```

Ans: 1,622


## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```
CREATE OR REPLACE TABLE
  pristine-sphere-412713.ny_taxi.green_tripdata_partitioned_clustered
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY
  PULocationID AS
SELECT
  *
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned;
```

Ans: Partition by lpep_pickup_datetime  Cluster on PUlocationID


## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

```
SELECT
  DISTINCT PULocationID
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned
WHERE
  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```
```
SELECT
  DISTINCT PULocationID
FROM
  pristine-sphere-412713.ny_taxi.external_green_tripdata
WHERE
  lpep_pickup_datetime BETWEEN (
  SELECT
    UNIX_MICROS(CAST('2022-06-01 00:00:00' AS TIMESTAMP)) * 1000 )
  AND (
  SELECT
    UNIX_MICROS(CAST('2022-06-30 00:00:00' AS TIMESTAMP)) * 1000 );
```

Ans: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

Ans: GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

Ans: False


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
```
SELECT
  COUNT(*)
FROM
  pristine-sphere-412713.ny_taxi.green_tripdata_nonpartitioned;
```

Ans: 0B

 
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3