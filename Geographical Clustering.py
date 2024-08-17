import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import math
import csv
import matplotlib.pyplot as plt
import random

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1/10000, lon1/10000, lat2/10000, lon2/10000])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(abs(a)))
    r = 6371
    distance = c * r
    return distance*1000

def binary_search(arr, target, idx):
    left, right = 0, len(arr) - 1
    while left <= right:
        mid = (left + right) // 2
        mid_latitude = arr[mid][0][0][idx]
        if mid_latitude == target:
            return mid
        elif mid_latitude < target:
            left = mid + 1
        else:
            right = mid - 1
    return -1

def recursion(i, c, prev):
    if visited[i[1]] == 0:
        visited[i[1]] = prev
        if prev in colorer:
            colorer[prev].append(i[4])
        else:
            colorer[prev] = [i[4]]
        stop_count[i[2]][2] = prev
        clat = i[0][0]
        clon = i[0][1]
        idx = binary_search(lat_sorted_arr, clat, 0)
        tidx = idx+1
        while tidx < len(lat_sorted_arr) and tidx < len(long_sorted_arr) and lat_sorted_arr[tidx][0][0][0] <= clat + 50:
            blat = lat_sorted_arr[tidx][0][0][0]
            blon = lat_sorted_arr[tidx][0][0][1]
            bidx = lat_sorted_arr[tidx][0][1]
            dist = haversine(blat, blon, clat, clon)
            if dist <= 50 and visited[lat_sorted_arr[tidx][0][1]] == 0 and visited[lat_sorted_arr[tidx][0][1]] == 0:
                recursion(lat_sorted_arr[tidx][0], c+1, prev)
            tidx += 1
        tidx = idx - 1
        while tidx >= 0 and lat_sorted_arr[tidx][0][0][0] >= clat - 50 and visited[lat_sorted_arr[tidx][0][1]] == 0:
            blat = lat_sorted_arr[tidx][0][0][0]
            blon = lat_sorted_arr[tidx][0][0][1]
            bidx = lat_sorted_arr[tidx][0][1]
            dist = haversine(blat, blon, clat, clon)
            if dist <= 50:
                recursion(lat_sorted_arr[tidx][0], c+1, prev)
            tidx -= 1
        tidx = binary_search(long_sorted_arr, clon, 1)
        while tidx < len(long_sorted_arr) and long_sorted_arr[tidx][0][0][1] <= clon + 50 and visited[lat_sorted_arr[tidx][0][1]] == 0:
            blat = long_sorted_arr[tidx][0][0][0]
            blon = long_sorted_arr[tidx][0][0][1]
            bidx = long_sorted_arr[tidx][0][1]
            dist = haversine(blat, blon, clat, clon)
            if dist <= 50:
                recursion(long_sorted_arr[tidx][0], c+1, prev)
            tidx += 1
        tidx = binary_search(long_sorted_arr, clon, 1) - 1
        while tidx >= 0 and long_sorted_arr[tidx][0][0][1] >= clon - 50 and visited[lat_sorted_arr[tidx][0][1]] == 0:
            blat = long_sorted_arr[tidx][0][0][0]
            blon = long_sorted_arr[tidx][0][0][1]
            bidx = long_sorted_arr[tidx][0][1]
            dist = haversine(blat, blon, clat, clon)
            if dist <= 50:
                recursion(long_sorted_arr[tidx][0], c+1, prev)
            tidx -= 1
            
            
df = pd.read_csv("stop_data.csv")
df.head()
stop_count = {}
lat = df['Latitude']
lon = df['Longitude']

for i in range(len(lat)):
    coord = int(lat[i] * 10000) * 1000000 + int(lon[i] * 10000)
    if coord in stop_count:
        stop_count[coord][1].append(i)
    else:
        temp = [int(lat[i] * 10000), int(lon[i] * 10000)]
        stop_count[coord] = [temp, [i], coord]

new_arr = []
counter = 0
for j, i in stop_count.items():
    for k in i[1]:
        new_arr.append([[[i[0][0], i[0][1]], k, i[2], counter, j]])
        counter+=1

lat_sorted_arr = sorted(new_arr, key=lambda item: (item[0][0], item[0][1]))
long_sorted_arr = sorted(new_arr, key=lambda item: (item[0][1], item[0][0]))

visited = np.full(len(df), 0)
temp_stop_lst = []
colorer = {}
c = 0
for i in range(len(lat_sorted_arr)):
    if visited[lat_sorted_arr[i][0][1]] == 0:
        recursion(lat_sorted_arr[i][0], 0, lat_sorted_arr[i][0][2])

df['stop_group'] = np.nan
for key, value in colorer.items():
    for i in value:
        df.at[stop_count[i][1], 'stop_group'] = int(key)
        #df['stop_group'].iloc[stop_count[i][1]] = int(key)    ###Uncomment this code if the line above does not work. Will result in warning
        lat = stop_count[i][0][0]/10000
        lon = stop_count[i][0][1]/10000

df.to_csv('stop_group_output.csv', index=False) ### Rename