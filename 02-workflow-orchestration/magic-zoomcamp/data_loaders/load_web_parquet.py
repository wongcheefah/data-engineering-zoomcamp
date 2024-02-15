import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    df_by_month = []

    fleet = kwargs['fleet']
    year = kwargs['year']

    for month in range(1, 13):
        filename = f'{fleet}_tripdata_{year}-{month:02d}.parquet'
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
        print(f'Loading {fleet} fleet {year}-{month} trip data')
        response = requests.get(url)
        df = pd.read_table(io.BytesIO(response.content)).to_pandas()
        df_by_month.append(df)

    return pd.concat(df_by_month, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
