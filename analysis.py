import pandas as pd
import geopandas as gpd
import json
import datetime
import numpy as np


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

monumentos2 = pd.read_excel('./data/monumentos.xlsx')
monumentos2 = gpd.GeoDataFrame(monumentos, geometry=gpd.points_from_xy(monumentos.longitud, monumentos.latitud),
                              crs={'init' :'epsg:4326'})
monumentos2 = monumentos.to_crs(epsg=3763)
monumentos2['geometry'] = monumentos2.buffer(150)
monumentos2 = monumentos2.to_crs(epsg=4326)

join2 = gpd.sjoin(gdf, monumentos2, how='left', op='intersects')
cidade = join2[join2['Nombre']=='Cidade da Cultura']
cidade['id'].unique()

join.iloc[join['Nombre'].isnull().values, join.columns.get_indexer([
    'Nombre'])] = 'Sen identificar'
join.iloc[join['id'].isin(cidade['id'].unique()).values, join.columns.get_indexer([
    'Nombre'])] = 'Cidade da Cultura'

join.to_excel('/mnt/c/Users/Pablo/Desktop/Santiagojoin.xlsx')


df = pd.read_excel('./data/join.xlsx')

group = join.groupby('Nombre')
count = group.count()['id']
total_rel_count = (count/953) * 100
relative_rel_count = (count/480) * 100
