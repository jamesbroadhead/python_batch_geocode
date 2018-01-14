import pandas as pd

from geo_enum import FIXED_ADDRESS

def load_input(filename, address_column_name, county_column_name, country):
    # Read the data to a Pandas Dataframe
    data = pd.read_csv(filename, encoding='utf8')

    if address_column_name not in data.columns:
        raise ValueError("Missing Address column in input data")

    for row in data:
        try:
            row[FIXED_ADDRESS] = fix_address(row_series, address_column_name, county_column_name, country)
        except Exception:
            pass

    return data

def fix_address(row, address_column_name, county_column_name, country):
    return row[address_column_name] + ',' + row[county_column_name] + ',' + country,


def write_output(output_filename, dataframe):
    return dataframe.to_csv(output_filename, encoding='utf-8')
