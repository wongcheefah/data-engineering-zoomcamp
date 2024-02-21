{{
    config(
        materialized='table'
    )
}}

with green_yellow_trip_ids as (
    select service_type
    from dbt_cwong.fact_trips
    where pickup_datetime between '2019-07-01' and '2019-07-31' and dropoff_datetime between '2019-07-01' and '2019-07-31'
), 
fhv_trip_ids as (
    select service_type
    from dbt_cwong.fact_fhv_trips
    where pickup_datetime between '2019-07-01' and '2019-07-31' and dropoff_datetime between '2019-07-01' and '2019-07-31'
),
trip_ids_unioned as (
    select * from green_yellow_trip_ids
    union all 
    select * from fhv_trip_ids
)
select
    service_type,
    count(service_type) as trips
from 
    trip_ids_unioned
group by 
    service_type