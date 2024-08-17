import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime
import math
import csv
import matplotlib.pyplot as plt
from scipy import stats
import folium
import random
from datetime import datetime
from sklearn.cluster import KMeans

df = pd.read_csv("stop_group_output40608.csv")
df.head()

columns = [
    'after_distance', 'after_time', 'after_speed', 'before_distance', 'before_time', 'before_speed', 
    'latitude', 'longitude', 'timestamp_start', 'timestamp_end', 'instant_speed', 'before_instant_speed', 
    'after_instant_speed', 'unique_id', 'Day', 'Vehicle_Type'
]

stop_group = {}
for i in range(len(df)):
    group_id = df['stop_group'].iloc[i]*10
    temp = df.loc[i, columns].values

    if group_id in stop_group:
        stop_group[group_id][0].append(temp)
    else:
        stop_group[group_id] = [[temp]]
        
        
total_time = 0
for key, value in stop_group.items():
    lcv_count = 0
    truck_count = 0
    avg = 0
    weekend = 0
    weekday = 0
    holiday = 0
    duration = 0
    time_zone = []
    time1 = 0
    time2 = 0
    time3 = 0
    time4 = 0
    time5 = 0
    unq_cars = set()
    #began may21th, Sunday
    for temp in value[0]:
        if temp[15] == 'LCV':
            lcv_count+=1
        else:
            truck_count+=1
        #if temp[14] == 
        if temp[14]%7 == 0 or (temp[14]+6)%7 == 0:
            weekend+=1
        else:
            weekday+=1
        unq_cars.add(temp[13])

        time_format = "%Y-%m-%d %H:%M:%S%z"
        dt1 = datetime.strptime(str(temp[8]), time_format)
        dt2 = datetime.strptime(str(temp[9]), time_format)
        time_difference = (dt2 - dt1).total_seconds()
        duration+=time_difference
        total_time+=time_difference
        time = dt1.time()
        
        if time >= datetime.strptime("05:00:00", "%H:%M:%S").time() and time < datetime.strptime("08:00:00", "%H:%M:%S").time():
            time_range = 0
            time1+=1
        elif time >= datetime.strptime("08:00:00", "%H:%M:%S").time() and time < datetime.strptime("16:00:00", "%H:%M:%S").time():
            time_range = 1
            time2+=1
        elif time >= datetime.strptime("16:00:00", "%H:%M:%S").time() and time < datetime.strptime("19:00:00", "%H:%M:%S").time():
            time_range = 2
            time3+=1
        elif time >= datetime.strptime("19:00:00", "%H:%M:%S").time() and time <= datetime.strptime("23:59:59", "%H:%M:%S").time():
            time_range = 3
            time4+=1
        elif time >= datetime.strptime("00:00:00", "%H:%M:%S").time() and time <= datetime.strptime("05:59:59", "%H:%M:%S").time():
            time_range = 3
            time4+=1
        time_zone.append(time_range)
    arr = [len(value[0]), len(unq_cars)/len(value[0]), lcv_count/(lcv_count+truck_count), weekday/(weekday+weekend), (duration/len(value[0])), time1/len(time_zone), time2/len(time_zone), time3/len(time_zone), time4/len(time_zone), time5/len(time_zone)]
    value.append(arr)
    
    
    
############
names = ["total_stops", "unq_count", "lcv_percent", "weekday_percent", "duration_avg", "5AM_8AM_percent", "8AM_4PM_percent", "4PM_7PM_percent", "7PM_12AM_percent", "12AM_5PM_precent"]

for ii in range(10):
    list_keys = list(stop_group.keys())
    data_points = np.array([stop_group[key][1][ii] for key in list_keys]).reshape(-1, 1)
    
    k = 20
    
    kmeans = KMeans(n_clusters=k, random_state=0).fit(data_points)
    
    labels = kmeans.labels_
    count_arr1 = np.zeros(k)
    count_arr2 = np.zeros(k)
    clustered_dict = {}
    for i, key in enumerate(list_keys):
        if labels[i] not in clustered_dict:
            clustered_dict[labels[i]] = []
        clustered_dict[labels[i]].append(key)
    avg_arr = np.zeros(k)
    
    it = 0
    
    cluster_ids = []
    stop_group_ids = []
    unique_ids = []
    latitudes = []
    longitudes = []
    
    for cluster_id in range(k):
        clustered_keys = [key for i, key in enumerate(list(stop_group.keys())) if labels[i] == cluster_id]
        num_stops = 0
        count_arr1[cluster_id] += len(clustered_keys)
        for key in clustered_keys:
            avg_arr[it] += stop_group[key][1][ii]
            num_stops += len(stop_group[key][0])
            count_arr2[cluster_id] += len(stop_group[key][0])
            for item in stop_group[key][0][1:]:
                lat = item[6]
                lon = item[7]
                cluster_ids.append(cluster_id)
                stop_group_ids.append(int((key * 10) / 10))
                unique_ids.append(item[14])
                latitudes.append(lat)
                longitudes.append(lon)
        
        if ii == 2 or ii == 3 or ii >= 5:
            avg_arr[it] /= len(clustered_keys)
        elif ii == 4:
            avg_arr[it] /= num_stops
            avg_arr[it] /= 60
            
        it += 1
    
    np.set_printoptions(precision=3, suppress=True)
    columns = [names[ii], "stop group count", "total count"]
    
    avg_arr = avg_arr.reshape(-1, 1)
    count_arr1 = count_arr1.reshape(-1, 1)
    count_arr2 = count_arr2.reshape(-1, 1)
    
    combined_arr = np.hstack((avg_arr, count_arr1, count_arr2))
    df2 = pd.DataFrame(combined_arr, columns=columns)
    str_name = "avg_arr_" + names[ii] + ".csv"
    df2.to_csv(str_name, index=False)
    
    df = pd.DataFrame({
        "cluster_ids": cluster_ids,
        "stop_group_ids": stop_group_ids,
        "unique_ids": unique_ids,
        "latitudes": latitudes,
        "longitudes": longitudes
    })
    str_name2 = "FFcluster_test_" + names[ii] + ".csv"
    df.to_csv(str_name2, index=False)