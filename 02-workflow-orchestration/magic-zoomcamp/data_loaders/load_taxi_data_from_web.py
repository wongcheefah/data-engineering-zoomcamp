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
