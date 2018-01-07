import pandas as pd



def load_input(filename, address_column_name, county_column_name, country):
    # Read the data to a Pandas Dataframe
    data = pd.read_csv(filename, encoding='utf8')

    if address_column_name not in data.columns:
        raise ValueError("Missing Address column in input data")


    output = []

    for _index, row_series in data.iterrows():
        line = { k: v for k, v in row_series.items() }

        try:
            line['FIXED_ADDRESS'] = fix_address(row_series, address_column_name, county_column_name, country)
        except Exception:
            print('Could not load an address from: {}'.format(row_series))
        output.append(line)

    return output

def fix_address(row, address_column_name, county_column_name, country):
    return row[address_column_name] + ',' + row[county_column_name] + ',' + country,

