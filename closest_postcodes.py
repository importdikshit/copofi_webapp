import pandas as pd
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')

zips = pd.read_csv('zips_coordinates.csv')


def eu(l1, l2):
    return np.sqrt(sum((np.array(l1)-np.array(l2))**2))

def k_closest_postcodes(k, lat, lon):
    filtered = zips[(zips['lat'] > lat-0.05) & (zips['lat'] < lat+0.05) & (zips['long'] > lon-0.05) & (zips['long'] < lon+0.05)]
    dist = []
    for i, j in zip(filtered['lat'], filtered['long']):
        dist.append(eu([lat, lon], [i,j]))
    filtered['dist'] = dist
    return list(filtered.sort_values("dist", ascending=True)['pcd'])[:k]

def get_zip_coords(post_code):
    return list(zips[zips['pcd']==post_code]['lat'])[0], list(zips[zips['pcd']==post_code]['long'])[0]