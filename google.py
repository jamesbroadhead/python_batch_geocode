import asyncio
import concurrent.futures
import requests
from db import update_record

class OverQueryLimit(Exception):
    pass

def get_google_result_async(remaining_work, config):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_get_google_result_async(remaining_work,
        api_key=config['api_key'], return_full_response=config['return_full_response'], num_workers=config['num_workers']))


async def _get_google_result_async(remaining_work, api_key, return_full_response, num_workers):

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:

        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                requests.get,
                get_google_result_and_write_to_db(work, api_key=api_key, return_full_response=return_full_response)
            )
            for work in remaining_work
        ]

        return concurrent.futures.wait(futures)


def get_google_result_and_write_to_db(work, api_key=None, return_full_response=False):
    if not 'FIXED_ADDRESS' in work:
        # no address - skip
        return None

    try:
        geocode = get_google_result(work['FIXED_ADDRESS'], api_key=api_key, return_full_response=return_full_response)
        return update_record(work, geocode)
    except OverQueryLimit:
        print('Failed - Over the Query Limit. Try again later!')
    except Exception as e:
        print('Failed for some other reason: {}'.format(e.args))



def get_google_result(address, api_key=None, return_full_response=False):
    """
    Get geocode results from Google Maps Geocoding API.

    Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.

    @param address: String address as accurate as possible. For Example "18 Grafton Street, Dublin, Ireland"
    @param api_key: String API key if present from google.
                    If supplied, requests will use your allowance from the Google API. If not, you
                    will be limited to the free usage of 2500 requests per day.
    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)

    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()

    if results['status'] == 'OVER_QUERY_LIMIT':
        raise OverQueryLimit()

    # if there's no results or an error, return empty results.
    if not results['results']:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
    else:
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components')
                                  if 'postal_code' in x.get('types')])
        }

    # Append some other details:
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True:
        output['response'] = results

    return output
