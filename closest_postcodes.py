import pandas as pd
import time
import numpy as np

zips = pd.read_csv('zips_coordinates.csv')

def eu(l1, l2):
    return np.sqrt(sum((np.array(l1)-np.array(l2))**2))

def k_closest_postcodes(k, zipcode):
    if zipcode not in list(zips['pcd']):
	     raise ValueError("post code not found")
    coords = zips[zips['pcd']==zipcode]
    coords = np.array(coords.iloc[0])[1:]
    dist = []
    for i, j in zip(zips['lat'], zips['long']):
        dist.append(eu(coords, [i,j]))
    zips['dist'] = dist
    return list(zips.sort_values("dist", ascending=True)['pcd'])[:k]