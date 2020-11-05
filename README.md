Task
1. Create a program that produces a typed parquet file (https://parquet.apache.org/) from this file:
    https://nyc-tlc.s3.us-east-1.amazonaws.com/trip%20data/green_tripdata_2013-09.csv
* By typed we mean that the data is stored it a format that is correct for its type. For example, times are stored as timestamps, numbers as ints, doubles etc.
* If you are unable to produce a parquet file use an alternative file format that also is strongly typed.
* If you are short on time focus on typing the pick up and drop off times as well as the pick up and drop off locations.

2. Create a derived dataset, from the one created above, using a SQL statement that selects all existing columns and adds these new columns:
* One-Hot encoding for each hour of the day
* One-Hot encoding for each day	of the week
* Duration in seconds of the trip
* An int encoding to indicate if the pickup or dropoff locations were at JFK airport. (Use a bounding box from the GPS coordinates to determine this). Provide pseudo code if out of time. This column is optional.

Save the new dataset to parquet.

Requirement
- Install python3
- Install apache-spark (3.0.0)
- Install pyspark

Task 1:
* Using pyspark read csv
* Rename column to underscore
* Save dataset to 'green_trip_data.parquet'

Task 2:
* Parquet files can also be used to create a temporary view and then used in SQL statements.
* SQL Statements:
    - One-Hot encoding: extract (HOUR, DAYOFWEEK), then use conditional statements
    - Duration of the trip: dropoff_datetime - pickup_datetime
    - An int encoding to indicate if the pickup or dropoff locations were at JFK airport: 
        
        A(lat_a, lon_a), B(lat_b, lon_b), R = 6371 km
        => AB = arccos(sin(lat_a)*sin(lat_b) + cos(lat_a)*cos(lat_b)*cos(lon_a - lon_b))*R
        (https://en.wikipedia.org/wiki/Great-circle_distance)
        
        JFK airport(lat=40.6413111, lon=-73.7781391)
        (https://en.wikipedia.org/wiki/John_F._Kennedy_International_Airport)
        
        JFK airport(21.04km²) -> distance is 5km
        (https://www.airport-technology.com/features/largest-airports-america)
* Save dataset to 'green_trip_data_prep.parquet'
