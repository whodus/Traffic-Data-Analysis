# Traffic Data Analysis

Analysis of commercial vehicle GPS telemetry to identify where vehicles stop, group those
stops into recurring locations, and characterize what kind of activity each location
represents. Built during a data science internship at Urban Radar.

The dataset is fleet telemetry for two vehicle classes (trucks and LCVs — light commercial
vehicles) recorded across the Paris region, stored as hive-partitioned Parquet.

## Pipeline

The scripts run in sequence. Each stage writes a CSV that the next stage reads.

```
Parquet telemetry
      |
      |  Data Extract 1.py
      v
  stop_data.csv            per-stop feature table
      |
      |  Geographical Clustering.py
      v
  stop_group_output.csv    stops labeled with a shared location ID
      |
      |  Clustering.py
      v
  avg_arr_*.csv            behavioral profile per stop-location cluster
  FFcluster_test_*.csv
```

`Trip Analysis.py` is a separate, SQL-based approach to the same problem — see below.

### 1. `Data Extract 1.py` — stop detection and feature extraction

Walks a directory of hive-partitioned Parquet files (partitioned by day, with vehicle type
encoded in the filename) and reads each one with PyArrow.

For every GPS ping, computes movement features relative to the neighboring pings from the
same vehicle:

- distance before / after (haversine, in meters)
- elapsed time before / after (seconds)
- speed before / after (km/h)

Rows flagged as stops are extracted into a feature table with position, start and end
timestamps, instantaneous speed before and after the stop, vehicle ID, day, and vehicle
type.

**Output:** `stop_data.csv`

### 2. `Geographical Clustering.py` — spatial grouping of stops

Groups stop events that occur at effectively the same physical place, so that repeated
visits to one location are recognized as one location.

Implemented directly rather than with a clustering library:

- coordinates are quantized to 1e-4 degrees and hashed, so exact-duplicate positions collapse
- stop points are held in two arrays, one sorted by latitude and one by longitude
- for a given point, binary search finds its position in each sorted array, and the scan
  walks outward only while the coordinate stays within the threshold band
- true separation is confirmed with a haversine distance check against a 50 m radius
- a recursive traversal over unvisited neighbors assigns every connected stop the same
  group ID

The result is connected-component clustering over a 50 m neighborhood graph, with the
sorted arrays and binary search used to avoid comparing every point against every other
point.

**Output:** `stop_group_output.csv` (adds a `stop_group` column)

### 3. `Clustering.py` — behavioral profiling of stop locations

Aggregates each stop group into ten features describing how that location is used:

| Feature | Meaning |
| --- | --- |
| `total_stops` | number of stop events recorded there |
| `unq_count` | ratio of distinct vehicles to total stops (repeat visits vs. many vehicles) |
| `lcv_percent` | share of stops by light commercial vehicles rather than trucks |
| `weekday_percent` | share of stops on weekdays rather than weekends |
| `duration_avg` | mean stop duration |
| `5AM_8AM_percent` … `12AM_5AM_percent` | share of stops falling in each time-of-day band |

KMeans (k = 20) is then run over each feature to bucket locations by that dimension of
behavior, writing per-cluster averages alongside the member stop coordinates.

**Output:** `avg_arr_<feature>.csv`, `FFcluster_test_<feature>.csv`

### 4. `Trip Analysis.py` — SQL approach with OpenStreetMap context

An alternative implementation of stop detection that pushes the work into DuckDB rather
than iterating in Python, and adds map context to explain *why* a vehicle stopped.

- telemetry is exposed as a PyArrow hive-partitioned dataset with an explicit typed schema
  and read through DuckDB
- OpenStreetMap extract for Île-de-France is read directly from `.pbf` via `ST_READOSM`
- OSM amenity, shop, and building tags are collapsed into categories — food, parking, fuel,
  financial, postal/storage, disposal, transportation, education, retail, logistics
- stops are detected with window functions (`LEAD`/`LAG` partitioned by vehicle, ordered by
  timestamp): a stop is a span where speed drops below 4 and the gap to the next ping
  exceeds three minutes
- a spatial join (`ST_DWithin_Spheroid`, 10 m) attributes each stop to nearby amenities and
  counts stops per category per vehicle

This gives a per-vehicle profile of what kinds of destinations its stops cluster around.

## Running

```bash
pip install pandas numpy pyarrow scipy scikit-learn matplotlib folium duckdb
```

Paths are currently hardcoded. Before running, set:

- the input directory in `data_extract()` in `Data Extract 1.py`
- the dataset path, DuckDB path, and `.osm.pbf` path at the top of `Trip Analysis.py`
- the input filename in `Clustering.py` (expects the output of the previous stage)

Run in order:

```bash
python "Data Extract 1.py"
python "Geographical Clustering.py"
python Clustering.py
```

`Trip Analysis.py` runs independently of the other three.

## Notes

No telemetry data is included in this repository.
