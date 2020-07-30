import pandas as pd
import geopandas as gpd
import json
import datetime


with open('./data/santiago.json') as f:
    data = json.load(f)


def reestructure(row):
    toret = {}
    toret['id'] = row['id']
    toret['date'] = row['date']['$date']
    toret['title'] = row['Title']
    toret['tags'] = row['tags']
    toret['owner'] = row['owner']
    toret['owner_name'] = row['owner_name']
    toret['views'] = row['views']
    toret['latitude'] = row['latitude']
    toret['longitude'] = row['longitude']
    return toret


def get_gdf(json_data):
    df = [reestructure(elem) for elem in data]
    df = pd.DataFrame(df)
    df['date'] = pd.to_datetime(df['date'])
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    return gdf


gdf = get_gdf(data)
gdf.plot()