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

from db import get_collection, remaining_addresses, load_config
from google import get_google_result_async

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



# Set your input file here
input_filename = sys.argv[1]

# Set your output file name here.
output_filename = 'data/results_{}'.format(input_filename)



#------------------ DATA LOADING --------------------------------

config = load_config()
collection = get_collection()
f = remaining_addresses(collection)

loop = asyncio.get_event_loop()
remaining_work = loop.run_until_complete(f)
print('Remaining work: {}'.format(len(remaining_work)))

get_google_result_async(remaining_work, config)
