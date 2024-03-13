# Homework

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

From this MV, find the pair of taxi zones with the highest average trip time.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

Answer: 1

<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW trip_times AS
    SELECT
        tz1.Zone || ' - ' || tz2.Zone AS zone_pair,
        AVG(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
        MIN(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
        MAX(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
    FROM
        trip_data
            JOIN taxi_zone as tz1
                ON trip_data.PULocationID = tz1.location_id
            JOIN taxi_zone AS tz2
                ON trip_data.DOLocationID = tz2.location_id
    GROUP BY
        zone_pair;

SELECT
    *
FROM
    trip_times
ORDER BY
    avg_trip_time_minutes DESC
LIMIT 1;

--          zone_pair         | avg_trip_time_minutes | min_trip_time_minutes | max_trip_time_minutes
-- ---------------------------+-----------------------+-----------------------+-----------------------
--  Yorkville East - Steinway |           1439.550000 |           1439.550000 |           1439.550000
-- (1 row)
```

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

Answer: 4

<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW trip_counts AS
    SELECT
        tz1.Zone || ' - ' || tz2.Zone AS zone_pair,
        COUNT(*) AS trip_count,
        AVG(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
        MIN(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
        MAX(EXTRACT(EPOCH FROM (trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
    FROM
        trip_data
            JOIN taxi_zone as tz1
                ON trip_data.PULocationID = tz1.location_id
            JOIN taxi_zone AS tz2
                ON trip_data.DOLocationID = tz2.location_id
    GROUP BY
        zone_pair;

SELECT
    *
FROM
    trip_counts
ORDER BY
    avg_trip_time_minutes DESC
LIMIT 1;

--          zone_pair         | trip_count | avg_trip_time_minutes | min_trip_time_minutes | max_trip_time_minutes
-- ---------------------------+------------+-----------------------+-----------------------+-----------------------
--  Yorkville East - Steinway |          1 |           1439.550000 |           1439.550000 |           1439.550000
-- (1 row)
```

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

Answer: 2

<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW busiest_zones AS
    WITH t AS (
        SELECT
            MAX(tpep_pickup_datetime) AS latest_pickup_datetime
        FROM
            trip_data
    )
    SELECT
        tz1.Zone as pu_zone,
        COUNT(*) AS pickup_count
    FROM
        t,
        trip_data
            JOIN taxi_zone as tz1
                ON trip_data.PULocationID = tz1.location_id
    WHERE
            trip_data.tpep_pickup_datetime > (t.latest_pickup_datetime - INTERVAL '17' HOUR)
    GROUP BY
        pu_zone;

SELECT
    *
FROM
    busiest_zones
ORDER BY
    pickup_count DESC
LIMIT 3;

--        pu_zone       | pickup_count
-- ---------------------+--------------
--  LaGuardia Airport   |           19
--  Lincoln Square East |           17
--  JFK Airport         |           17
-- (3 rows)
```