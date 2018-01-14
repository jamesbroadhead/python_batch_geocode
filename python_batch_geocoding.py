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
from datetime import datetime, timedelta
import logging
import sys
import shutil
import os
from os.path import exists as pexists
from os.path import expanduser, isdir, abspath, dirname
import json
from db import get_collection, remaining_addresses, load_config
from google import get_google_result, OverQueryLimit
import math
from geo_csv import load_input, write_output, empty_cells
from geo_enum import GEOCODE, FIXED_ADDRESS
from numpy import NaN
import time

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
        parent = dirname(abspath(output_filename))

        if not isdir(parent):
            os.makedirs(parent)

        print('Partial results not found - starting from scratch')
        shutil.copyfile(input_filename, output_filename)

    print('Loading input')
    config = load_config()
    data = load_input(output_filename, config['address_column_name'], config['county_column_name'], config['country'])
    print('Loading complete')

    return data


def assess_failures(recent_failures):
    now = datetime.now()

    recent_failures.append(now)
    if len(recent_failures) > 1:
        recent_failures = recent_failures[1:4]

    if recent_failures[0] < now - timedelta(minutes=1):
        print('Taking a 1m break')
        time.sleep(60)
    else:
        print('Taking a 5s break')
        time.sleep(5)



last_3_failures = []
def main(input_filename):
    output_filename = 'data/results_{}'.format(input_filename)
    data = load_data(input_filename, output_filename)

    for index, row_namedtuple in enumerate(data.itertuples()):
        row = row_namedtuple._asdict()

        existing_geocode = row.get(GEOCODE, None)
        if existing_geocode not in empty_cells:
            # we have data already
            continue

        address = row.get(FIXED_ADDRESS, None)
        if address in empty_cells:
            print('Missing address')
            # no address - write a dummy value
            result = 'NO_ADDRESS'
        else:
            print('Calling google...')

            try:
                api_response = get_google_result(address, api_key=None, return_full_response=False)
                result = api_response['formatted_address']

                data.set_value(index, GEOCODE, result)

                if index > 0 and index % 10 == 0:
                    print('Saving partial results - progress: {}/{}'.format(index, len(data)))
                    write_output(output_filename, data)

            except OverQueryLimit as e:
                # NOTE: if this happens, we skip this address & will need to re-run the script later to fill it in
                print('Failed - Over the Query Limit - progress: {}/{} - {}'.format(index, len(data), e.args))
                write_output(output_filename, data)
                assess_failures(last_3_failures)

            except Exception as e:
                # NOTE: if this happens, we skip this address & will need to re-run the script later to fill it in
                print('Failed - Unknown error- progress: {}/{} - {}'.format(index, len(data), e.args))
                write_output(output_filename, data)
                assess_failures(last_3_failures)




if __name__ == '__main__':
    input_filename = sys.argv[1]
    main(input_filename)
