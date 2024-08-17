import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime
import math

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371
    distance = c * r
    return distance

def add_features(df):
    if df.empty:
        df['after_distance'] = []
        df['after_times'] = []
        df['after_speeds'] = []
        df['before_distances'] = []
        df['before_times'] = []
        df['before_speeds'] = []
        return df

    before_distances = []
    after_distances = []
    before_times = []
    after_times = []
    before_speeds = []
    after_speeds = []
    df.head()
    for i in range(len(df)):
        if i < len(df)-1 and df.iloc[i]['unique_id'] == df.iloc[i + 1]['unique_id']:
            lat1, lon1 = df.iloc[i]['latitude'], df.iloc[i]['longitude']
            lat2, lon2 = df.iloc[i + 1]['latitude'], df.iloc[i + 1]['longitude']
            dist = haversine(lat1, lon1, lat2, lon2)

            time = (datetime.fromisoformat(str(df.iloc[i + 1]['timestamp_start'])) - datetime.fromisoformat(str(df.iloc[i]['timestamp_start']))).total_seconds()
            after_times.append(time)

            if time == 0:
                sp = 0
            else:
                sp = dist / (time / 3600)
            after_speeds.append(sp)
            
            after_distances.append(dist)
        else:
            after_times.append(0)
            after_speeds.append(0)
            after_distances.append(0)
            
        if i > 0 and df.iloc[i]['unique_id'] == df.iloc[i - 1]['unique_id']:
            lat1, lon1 = df.iloc[i]['latitude'], df.iloc[i]['longitude']
            lat2, lon2 = df.iloc[i - 1]['latitude'], df.iloc[i - 1]['longitude']
            dist = haversine(lat1, lon1, lat2, lon2)
        
            time = (datetime.fromisoformat(str(df.iloc[i]['timestamp_start'])) - datetime.fromisoformat(str(df.iloc[i-1]['timestamp_start']))).total_seconds()
            before_times.append(time)

            if time == 0:
                sp = 0
            else:
                sp = dist / (time / 3600)
            before_speeds.append(sp)
            
            before_distances.append(dist)
        else:
            before_times.append(0)
            before_speeds.append(0)
            before_distances.append(0)

    

    df['after_distance'] = after_distances
    df['after_times'] = after_times
    df['after_speeds'] = after_speeds
    df['before_distances'] = before_distances
    df['before_times'] = before_times
    df['before_speeds'] = before_speeds
    return df    

def stop_lst_generator(df):
    temp = []
    temp2 = []
    temp3 = []
    temp4 = []
    temp5 = []
    temp6 = []
    temp7 = []
    temp8 = []
    temp9 = []
    temp10 = []
    temp11 = []
    temp12 = []
    temp13 = []
    temp14 = []
    
    for i in range(len(df) - 1):
        if df.iloc[i]['is_stop'] and df.iloc[i]['unique_id'] == df.iloc[i + 1]['unique_id']:
            temp.append(df.iloc[i]['after_distance'] * 1000)
            temp2.append(df.iloc[i]['after_times'])
            temp3.append(df.iloc[i]['after_speeds'])
            temp4.append(df.iloc[i]['before_distances'] * 1000)
            temp5.append(df.iloc[i]['before_times'])
            temp6.append(df.iloc[i]['before_times'])
            temp7.append(df.iloc[i]['latitude'])
            temp8.append(df.iloc[i]['longitude'])
            temp9.append(df.iloc[i]['timestamp_start'])
            temp10.append(df.iloc[i]['timestamp_end'])
            temp11.append(df.iloc[i]['instant_speed'])
            temp14.append(df.iloc[i]['unique_id'])
            if i > 0:
                temp12.append(df.iloc[i-1]['instant_speed'])
            else:
                temp12.append(df.iloc[i-1]['instant_speed'])
            temp13.append(df.iloc[i+1]['timestamp_start'])
    
    stop_lst = np.array(temp)
    stop_lst2 = np.array(temp2)
    stop_lst3 = np.array(temp3)
    stop_lst4 = np.array(temp4)
    stop_lst5 = np.array(temp5)
    stop_lst6 = np.array(temp6)
    stop_lst7 = np.array(temp7)
    stop_lst8 = np.array(temp8)
    stop_lst9 = np.array(temp9)
    stop_lst10 = np.array(temp10)
    stop_lst11 = np.array(temp11)
    stop_lst12 = np.array(temp12)
    stop_lst13 = np.array(temp13)
    stop_lst14 = np.array(temp14)
    
    data = {
        'after_distance': stop_lst,
        'after_time': stop_lst2,
        'after_speed': stop_lst3,
        'before_distance': stop_lst4,
        'before_time': stop_lst5,
        'before_speed': stop_lst6,
        'latitude': stop_lst7,
        'longitude': stop_lst8,
        'timestamp_start': stop_lst9,
        'timestamp_end': stop_lst10,
        'instant_speed': stop_lst11,
        'before_instant_speed': stop_lst12,
        'after_instant_speed': stop_lst13,
        'unique_id': stop_lst14
    }
    
    return data

def data_extract(directory):
    bulk_arr = []
    folder_list = []
    
    for folder in os.listdir(directory):
        if folder == '.DS_Store':
            continue
        space_idx = folder.find(" ")
        folder_list.append(folder[0:space_idx])
        day = folder.split("=")[1]
        folder_path = os.path.join(directory, folder)
    
        for filename in os.listdir(folder_path):
            if filename == '.DS_Store':
                continue
            else:
                file_path = os.path.join(folder_path, filename)
                df = pq.read_table(file_path).to_pandas()
                
                add_features(df)
                data = stop_lst_generator(df)
                df = pd.DataFrame(data)
                vehicle_type = 'Truck' if "vehicle_type=lcv" not in filename else 'LCV'
                df['Day'] = day
                df['Vehicle_Type'] = vehicle_type
                bulk_arr.append(df)
    
    all_data = pd.concat(bulk_arr, ignore_index=True)
    all_data.to_csv('stop_data.csv', index=False)
