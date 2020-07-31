import requests
import time
import json
import re
from pymongo import MongoClient
import pandas as pd

# STEP 1: Scrap posts for a tag
arr = []
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


for i in range(0, conf['page_count']):
    url = "https://www.instagram.com/explore/tags/{0}/?__a=1&max_id={1}".format(
        conf['tag'], end_cursor)
    r = requests.get(url)
    data = json.loads(r.text)

    # value for the next page
    end_cursor = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
    # list with posts
    edges = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']

    for item in edges:
       arr.append(item['node'])

    time.sleep(2)  # insurence to not reach a time limit

print(end_cursor)  # save this to restart parsing with the next page


for item in arr:
    shortcode = item['shortcode']
    url = "https://www.instagram.com/p/{0}/?__a=1".format(shortcode)

    r = requests.get(url)
    try:
        data = json.loads(r.text)
        df = pd.io.json.json_normalize(data, sep='_')
        data = df.to_dict(orient='records')[0]
        collection.insert_one(data)
    except:
        pass
