import requests
import time
import json
import re
from pymongo import MongoClient, GEOSPHERE
import pandas as pd
import datetime
import progressbar
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from functools import partial
from shapely.geometry import mapping


def format_data(data):
    data = data['graphql']['shortcode_media']
    data['taken_at_timestamp'] = datetime.datetime.fromtimestamp(
        int(data['taken_at_timestamp']))
    return data


def format_location(data, geolocator):
    dt = json.loads(data['location']['address_json'])
    geocode = partial(geolocator.geocode,
                      country_codes=dt['country_code'], geometry='geojson')
    loc = geocode(query=dt["city_name"])
    data['geometry'] = loc.raw['geojson']
    data['nominatin'] = loc.raw
    data['latitude'] = loc.latitude
    data['longitude'] = loc.longitude
    time.sleep(1.1)
    return data


with open('./conf.json', 'r') as f:
    conf = json.load(f)

geolocator = Nominatim(user_agent="pablo.coellopulido@usc.es")
end_cursor = conf['end_cursor']

if re.search(' ', conf['database']) is None and len(conf['database']) < 20:
    if re.search(' ', conf['collection']) is None and len(conf['collection']) < 20:

        client = MongoClient('localhost', 27017)
        db = eval('client.' + conf['database'])
        collection = eval('db.' + conf['collection'])
        collection.create_index([("geometry", GEOSPHERE)])
    else:
        print('invalid collection name')
else:
    print('invalid database name')


for i in progressbar.progressbar(range(0, conf['page_count'])):
    arr = []
    url = "https://www.instagram.com/explore/tags/{0}/?__a=1&max_id={1}".format(
        conf['tag'], end_cursor)
    r = requests.get(url)
    data = json.loads(r.text)

    # value for the next page
    end_cursor = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
    # list with posts
    edges = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']

    time.sleep(2)  # insurence to not reach a time limit

    if len(edges) > 0:
        for item in edges:
            arr.append(item['node'])

        for item in arr:
            shortcode = item['shortcode']
            url = "https://www.instagram.com/p/{0}/?__a=1".format(shortcode)

            r = requests.get(url)
            try:
                data = json.loads(r.text)
                data = format_data(data)
                if len(data['location']['address_json']) > 0:
                    data = format_location(data, geolocator)
                    collection.insert_one(data)
            except:
                pass
client.close()


conf['end_cursor'] = end_cursor
with open('conf.json', 'w') as outfile:
    json.dump(conf, outfile)
