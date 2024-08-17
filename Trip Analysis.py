import pyarrow
import duckdb
import os
import sqlite3
import pandas as pd
import numpy as np
import spatial
import pyarrow as pa
import pyarrow.dataset as ds

bms_schema =  pa.schema(
    [('longitude', pa.float32()),
    ('latitude', pa.float32()),
    ('speed', pa.int32()),
    ('heading', pa.int32()),
    ('vehicle_type', pa.string()),
    ('road_type', pa.string()),
    ('road_speed_limit',pa.int16()),
    ('hour', pa.int8()),
    ('unique_id',pa.string()),
    ('timestamp', pa.timestamp(unit="ns", tz= "Europe/Paris")),
    ('month', pa.int32()),
    ('day',pa.int32())
])

bms_con = ds.dataset("C:/workspace/data", partitioning = 'hive', schema = bms_schema)
duck_con = duckdb.connect("C:/workspace/temp.db")
rel = duck_con.from_arrow(bms_con)


duck_con.sql("INSTALL spatial;")
duck_con.sql("LOAD spatial;")

osm_raw = duck_con.sql("SELECT * FROM ST_READOSM('C:/workspace/ile-de-france-latest.osm.pbf')")



osm_amenities = (
    osm_raw.
    select("""
    *,
    CASE 
        WHEN map_extract(tags, 'amenity')[1] IN ('restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'ice_cream') THEN 'food_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('parking', 'parking_space') OR map_extract(tags, 'amenity')[1] LIKE '%parking%' THEN 'parking_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('fuel', 'charging_station') THEN 'fuel_station'
        WHEN map_extract(tags, 'amenity')[1] IN ('bank', 'atm') THEN 'financial_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('luggage_locker', 'parcel_locker', 'post_office', 'post_box') THEN 'postal_storage_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('waste_disposal', 'waste_basket', 'recycling', 'waste_transfer_station') THEN 'disposal_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('car_rental', 'bicycle_rental', 'taxi', 'ferry_terminal') THEN 'transportation_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('training', 'library', 'college', 'language_school', 'school', 'dancing_school', 'university', 'kindergarten', 'driving_school') THEN 'educational_facility'
        WHEN map_extract(tags, 'shop')[1] IS NOT NULL THEN 'shop'
        WHEN map_extract(tags, 'building')[1] IN ('commercial', 'industrial', 'retail', 'supermarket', 'office') THEN 'commercial_building'
        WHEN map_extract(tags, 'building')[1] IN ('warehouse') THEN 'logistics'
        ELSE NULL
    END as category,
    st_point(lat, lon) AS geometry
    """).
    filter("""
    (map_extract(tags, 'amenity')[1] IS NOT NULL OR map_extract(tags, 'barrier')[1] IS NOT NULL OR map_extract(tags, 'building')[1] IS NOT NULL OR map_extract(tags, 'shop')[1] IS NOT NULL) AND
    lon BETWEEN 2.2508 AND 2.4130 AND
    lat BETWEEN 48.8162 AND 48.9083
    """).
    set_alias("osm_amenities")
)

session_ids = rel.filter(
    """
    longitude >= 2.2508 AND
    longitude <= 2.4130 AND
    latitude >= 48.8162 AND
    latitude <= 48.9083 AND
    month == 7 AND day == 5
    """).project('unique_id').distinct()


trips_sessions = (
    rel.filter(
        """
        longitude >= 2.2508 AND
        longitude <= 2.4130 AND
        latitude >= 48.8162 AND
        latitude <= 48.9083 AND
        month == 7 AND day == 5
        """
    ).select(
        """
        *,
        LEAD(timestamp) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_timestamp,
        LEAD(latitude) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_latitude,
        LEAD(longitude) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_longitude,
        LAG(speed) OVER(PARTITION BY unique_id ORDER BY timestamp) AS prev_speed
        """
    ).select(
        """
        *,
        LEAST(speed, prev_speed) AS min_speed,
        age(next_timestamp, timestamp) AS duration,
        ST_Distance_Sphere(ST_Point(longitude, latitude), ST_Point(next_longitude, next_latitude)) AS distance
        """
    ).filter(
        """
        min_speed < 4 AND
        duration > INTERVAL '3 minutes'
        """
    ).select("*, st_point(latitude, longitude) AS geometry, row_number() OVER() AS row_id")
    .set_alias("trip_points")
)

amenities_by_trip = (
    trips_sessions.project("unique_id, row_id, geometry")
    .join(
        osm_amenities.project("geometry, category"),
        condition="st_dwithin_spheroid(trip_points.geometry, osm_amenities.geometry, 10)",
        how='left'
    )
    .aggregate(
        """
        unique_id,
        COUNT(CASE WHEN category = 'disposal_facility' THEN 1 END) AS disposal_facility_stops,
        COUNT(CASE WHEN category = 'food_facility' THEN 1 END) AS food_facility_stops,
        COUNT(CASE WHEN category = 'parking_facility' THEN 1 END) AS parking_facility_stops,
        COUNT(CASE WHEN category = 'fuel_station' THEN 1 END) AS fuel_station_stops,
        COUNT(CASE WHEN category = 'financial_facility' THEN 1 END) AS financial_facility_stops,
        COUNT(CASE WHEN category = 'postal_storage_facility' THEN 1 END) AS postal_storage_facility_stops,
        COUNT(CASE WHEN category = 'transportation_facility' THEN 1 END) AS transportation_facility_stops,
        COUNT(CASE WHEN category = 'educational_facility' THEN 1 END) AS educational_facility_stops,
        COUNT(CASE WHEN category = 'commercial_building' THEN 1 END) AS commercial_building_stops,
        COUNT(CASE WHEN category = 'warehouse' THEN 1 END) AS logistics_stop,
        COUNT(CASE WHEN category = 'shop' THEN 1 END) AS shop_stop
        """,
        "unique_id"
    )
)
amenities_by_trip_df = amenities_by_trip.df()



filtered_amenities_by_trip_df = amenities_by_trip_df[
    (amenities_by_trip_df['disposal_facility_stops'] > 0) |
    (amenities_by_trip_df['food_facility_stops'] > 0) |
    (amenities_by_trip_df['parking_facility_stops'] > 0) |
    (amenities_by_trip_df['fuel_station_stops'] > 0) |
    (amenities_by_trip_df['financial_facility_stops'] > 0) |
    (amenities_by_trip_df['postal_storage_facility_stops'] > 0) |
    (amenities_by_trip_df['transportation_facility_stops'] > 0) |
    (amenities_by_trip_df['educational_facility_stops'] > 0) |
    (amenities_by_trip_df['commercial_building_stops'] > 0) |
    (amenities_by_trip_df['logistics_stop'] > 0) |
    (amenities_by_trip_df['shop_stop'] > 0)
]

filtered_amenities_by_trip_df.to_csv("final_test2.csv")

unique_id = filtered_amenities_by_trip_df['unique_id']

df_numeric = filtered_amenities_by_trip_df.drop('unique_id', axis=1)

df_percentages = df_numeric.div(df_numeric.sum(axis=1), axis=0) * 100

df_percentages = df_percentages.round(2)

df_percentages.insert(0, 'unique_id', unique_id)

filtered_amenities_by_trip_df.to_csv("final_test2.csv")

chosen_unique_id = '0ce5f82596554d10a634efecb49ac007a29fe2b5'

osm_amenities = (
    osm_raw.
    select("""
    *,
    st_point(lat, lon) AS geometry,
    CASE
        WHEN map_extract(tags, 'amenity')[1] IN ('restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'ice_cream') THEN 'food_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('parking', 'parking_space') OR map_extract(tags, 'amenity')[1] LIKE '%parking%' THEN 'parking_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('fuel', 'charging_station') THEN 'fuel_station'
        WHEN map_extract(tags, 'amenity')[1] IN ('bank', 'atm') THEN 'financial_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('luggage_locker', 'parcel_locker', 'post_office', 'post_box') THEN 'postal_storage_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('waste_disposal', 'waste_basket', 'recycling', 'waste_transfer_station') THEN 'disposal_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('car_rental', 'bicycle_rental', 'taxi', 'ferry_terminal') THEN 'transportation_facility'
        WHEN map_extract(tags, 'amenity')[1] IN ('training', 'library', 'college', 'language_school', 'school', 'dancing_school', 'university', 'kindergarten', 'driving_school') THEN 'educational_facility'
        WHEN map_extract(tags, 'shop')[1] IS NOT NULL THEN 'shop'
        WHEN map_extract(tags, 'building')[1] IN ('commercial', 'industrial', 'retail', 'supermarket', 'office') THEN 'commercial_building'
        WHEN map_extract(tags, 'building')[1] IN ('warehouse') THEN 'logistics'
        ELSE NULL
    END AS type
    """).
    filter("""
    lon BETWEEN 2.2508 AND 2.4130 AND
    lat BETWEEN 48.8162 AND 48.9083
    """).
    set_alias("osm_amenities")
)

trip_points = (
    rel.filter(
        f"""
        longitude >= 2.2508 AND
        longitude <= 2.4130 AND
        latitude >= 48.8162 AND
        latitude <= 48.9083 AND
        month == 7 AND day == 5 AND
        unique_id == '{chosen_unique_id}'
        """
    ).select(
        """
        *,
        LEAD(timestamp) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_timestamp,
        LEAD(latitude) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_latitude,
        LEAD(longitude) OVER(PARTITION BY unique_id ORDER BY timestamp) AS next_longitude,
        LAG(speed) OVER(PARTITION BY unique_id ORDER BY timestamp) AS prev_speed
        """
    ).select(
        """
        *,
        LEAST(speed, prev_speed) AS min_speed,
        age(next_timestamp, timestamp) AS duration,
        ST_Distance_Sphere(ST_Point(longitude, latitude), ST_Point(next_longitude, next_latitude)) AS distance
        """
    ).filter(
        """
        min_speed < 4 AND
        duration > INTERVAL '3 minutes'
        """
    ).select("*, st_point(latitude, longitude) AS geometry, row_number() OVER() AS row_id")
    .set_alias("trip_points")
)

amenities_by_point = (
    trip_points.project("row_id, geometry").  
    join(
        osm_amenities.project("geometry, type"), 
        condition="st_dwithin_spheroid(trip_points.geometry, osm_amenities.geometry, 20)"
    ).
    aggregate(
        aggr_expr="row_id, array_agg(DISTINCT type) AS types_20m", 
        group_expr="row_id"
    )
)

trip_points_df = trip_points.df()
amenities_by_point_df = amenities_by_point.df()

trips_plus_df = pd.merge(trip_points_df, amenities_by_point_df, on='row_id', how='left')


print(trips_plus_df)