#!/usr/bin/env python
"""
Python script for batch geocoding of addresses using the Google Geocoding API.

This script allows for massive lists of addresses to be geocoded for free by pausing when the
geocoder hits the free rate limit set by Google (2500 per day).  If you have an API key for paid
geocoding from Google, set it in the API key section.

Addresses for geocoding can be specified in a list of strings "addresses". In this script, addresses
come from a csv file with a column "Address". Adjust the code to your own requirements as needed.

After every 500 successul geocode operations, a temporary file with results is recorded in case of
script failure / loss of connection later.

Addresses and data are held in memory, so this script may need to be adjusted to process files line
by line if you are processing millions of entries.

Shane Lynn
5th November 2016
"""
import asyncio
import logging
import sys
import shutil
from os.path import exists as pexists
from os.path import expanduser
import json
from db import get_collection, remaining_addresses, load_config
from google import get_google_result

from geo_csv import load_input, write_output
from geo_enum import GEOCODE, FIXED_ADDRESS

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def load_config():
    with open(expanduser('~/.geocode')) as fh:
        return json.loads(fh.read())


def load_data(input_filename, output_filename):

    if not pexists(output_filename):
        print('Partial results not found - starting from scratch')
        shutil.copyfile(input_filename, output_filename)

    print('Loading input')
    config = load_config()
    data = load_input(output_filename, config['address_column_name'], config['county_column_name'], config['country'])
    print('Loading complete')

    return data


def main(input_filename):
    output_filename = 'data/results_{}'.format(input_filename)
    data = load_data(input_filename, output_filename)

    for index, row_namedtuple in enumerate(data.itertuples()):
        row = row_namedtuple._asdict()

        if row.get(GEOCODE, None) is not None:
            # we have data already
            continue

        address = row.get(FIXED_ADDRESS, None)
        if address is None:
            # no address - write a dummy value
            result = 'NO_ADDRESS'
        else:
            result = get_google_result(address, api_key=None, return_full_response=False)

        # row[GEOCODE] = result

        data.set_value(index, GEOCODE, result)

        if index > 0 and index % 10 == 0:
            print('Saving partial results - progress: {}/{}'.format(index, len(data)))
            write_output(output_filename, data)
            print('Saved')

        # TODO this limits rate-limit usage during dev
        if index > 50:
            sys.exit(0)



if __name__ == '__main__':
    input_filename = sys.argv[1]
    main(input_filename)
