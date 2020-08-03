import requests
import time
import json
import re
from pymongo import MongoClient
import pandas as pd
import datetime
import progressbar


def format_data(data):
    data = data['graphql']['shortcode_media']
    data['taken_at_timestamp'] = datetime.datetime.fromtimestamp(
        int(data['taken_at_timestamp']))
    return data

# STEP 1: Scrap posts for a tag
end_cursor = ''  # empty for the 1st page

with open('./conf.json', 'r') as f:
    conf = json.load(f)

if re.search(' ', conf['database']) is None and len(conf['database']) < 20:
    if re.search(' ', conf['collection']) is None and len(conf['collection']) < 20:

        client = MongoClient('localhost', 27017)
        db = eval('client.' + conf['database'])
        collection = eval('db.' + conf['collection'])
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

    if len(edges) > 0:
        for item in edges:
            arr.append(item['node'])

        time.sleep(2)  # insurence to not reach a time limit

        for item in arr:
            shortcode = item['shortcode']
            url = "https://www.instagram.com/p/{0}/?__a=1".format(shortcode)

            r = requests.get(url)
            try:
                data = json.loads(r.text)
                collection.insert_one(format_data(data))
            except:
                pass

print(end_cursor)  # save this to restart parsing with the next page
