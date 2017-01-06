import pandas as pd
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')

zips = pd.read_csv('zips_coordinates.csv')


def eu(l1, l2):
    return np.sqrt(sum((np.array(l1)-np.array(l2))**2))

def k_closest_postcodes(k, zipcode):
    
    if zipcode not in list(zips['pcd']):
	     raise ValueError("post code not found")
    coords = zips[zips['pcd']==zipcode]
    coords = np.array(coords.iloc[0])[1:]
    lat = coords[0]
    lon = coords[1]
    filtered = zips[(zips['lat'] > lat-0.05) & (zips['lat'] < lat+0.05) & (zips['long'] > lon-0.05) & (zips['long'] < lon+0.05)]
    dist = []
    for i, j in zip(filtered['lat'], filtered['long']):
        dist.append(eu(coords, [i,j]))
    filtered['dist'] = dist
    return list(filtered.sort_values("dist", ascending=True)['pcd'])[:k]