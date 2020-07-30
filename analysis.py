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
    df['date'] = pd.to_datetime(df['date']).dt.strftime("%d/%m/%Y %H:%M:%S")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    return gdf


gdf = get_gdf(data)
df = pd.DataFrame(gdf)
df.to_excel('./data/excel.xlsx')

monumentos = pd.read_excel('./data/monumentos.xlsx')
monumentos = gpd.GeoDataFrame(monumentos, geometry=gpd.points_from_xy(monumentos.longitud, monumentos.latitud),
                              crs={'init' :'epsg:4326'})
monumentos = monumentos.to_crs(epsg=3763)
monumentos['geometry'] = monumentos.buffer(50)
monumentos = monumentos.to_crs(epsg=4326)

join = gpd.sjoin(gdf, monumentos, how='inner', op='intersects')

group = join.groupby('Nombre')
count = group.count()['id']
total_rel_count = (count/953) * 100
relative_rel_count = (count/480) * 100

join.to_excel('./data/join.xlsx')
