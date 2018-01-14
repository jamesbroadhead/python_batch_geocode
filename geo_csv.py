import pandas as pd
import math
from numpy import NaN

from geo_enum import FIXED_ADDRESS

empty_cells = (None, math.nan, NaN)

def load_input(filename, address_column_name, county_column_name, country):
    # Read the data to a Pandas Dataframe
    data = pd.read_csv(filename, encoding='utf8')

    if address_column_name not in data.columns:
        raise ValueError("Missing Address column in input data")

    for index, row_namedtuple in enumerate(data.itertuples()):
        row = row_namedtuple._asdict()

        try:
            fixed_address = fix_address(row_series, address_column_name, county_column_name, country)
        except Exception:
            fixed_address = None

        data.set_value(index, FIXED_ADDRESS, fixed_address)

    return data

def fix_address(row, address_column_name, county_column_name, country):
    return row[address_column_name] + ',' + row[county_column_name] + ',' + country,


def write_output(output_filename, dataframe):
    """ skip columns with no name """

    columns = sorted({
        col for col in dataframe.columns.values
        if col not in empty_cells and
        not col.startswith('Unnamed') })

    return dataframe.to_csv(output_filename, encoding='utf-8', columns=columns)
