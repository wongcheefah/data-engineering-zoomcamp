import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def camel_to_snake(col_name: str) -> str:
    # Handle 'ID', 'PU', and 'DO' as special cases
    col_name = re.sub(
        r'(ID|PU|DO)(?=[A-Z]|$)', 
        lambda m: '_' + m.group(1).lower() + '_', 
        col_name
        )

    # Prefix capital letters with underscore and convert the string to lowercase
    col_name = re.sub(
        r'(?<=[^_])(?=[A-Z])', 
        '_', 
        col_name
        ).lower()

    # Clean up any double underscore and strip leading/trailing underscores
    col_name = col_name.replace('__', '_').strip('_')

    return col_name


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Assuming missing passenger_count or trip_distance value means zero
    selection = (data['passenger_count'].isna()) | (data['trip_distance'].isna()) | (data['passenger_count'] == 0) | (data['trip_distance'] == 0.0)

    print(f'Preprocessing {selection.sum()} rows with missing or zero passenger count or trip distance.')

    # Row selection, column creation and camel-to-snake-case conversion
    data = data[~selection]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.columns = [camel_to_snake(col) for col in data.columns]

    print(f'Transformation complete\n{data.shape[0]} rows x {data.shape[1]} columns')

    print(f'Unique vendor_id values: {data["vendor_id"].unique().tolist()}')

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns.to_list(), '"vendor_id" is not one of the columns.'
    assert (output['passenger_count'] > 0).all(), 'Not all passenger counts are greater than zero.'
    assert (output['trip_distance'] > 0).all(), 'Not all trip distances are greater than zero.'
