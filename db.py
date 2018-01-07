#!/usr/bin/env python
"""
Database class for the tool. If run directly, wipes & prepares the db

Usage:
    ./db.py INPUT_FILENAME

"""
from os.path import expanduser
import json
import asyncio
import docopt
import motor.motor_asyncio
from pymongo import InsertOne

from load_csv import load_input

GEOCODE = 'GEOCODE'

def load_config():
    with open(expanduser('~/.geocode')) as fh:
        return json.loads(fh.read())


def get_collection():
    config = load_config()
    client = motor.motor_asyncio.AsyncIOMotorClient(config['hostname'], config['port'])
    db = client[config['db']]
    return db[config['coll']]


async def load_collection(collection, data):
    await collection.drop()
    #await collection.ensure_index("address", unique=True)

    operations = [ InsertOne(data_obj) for data_obj in data]

    return await collection.bulk_write(operations)


async def remaining_addresses(collection):
    cursor = collection.find({ GEOCODE: {'$exists': False}})
    return await cursor.to_list(20000)


async def update_record(data, geocode):
    collection = get_collection()
    return collection.update(data, { '$set': { GEOCODE: geocode } }, upsert=False, multi=False)




def main(input_filename):
    config = load_config()
    data = load_input(input_filename, config['address_column_name'], config['county_column_name'], config['country'])

    collection = get_collection()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(load_collection(collection, data))



if __name__ == '__main__':
    args = docopt.docopt(__doc__)

    main(args['INPUT_FILENAME'])
