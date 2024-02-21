{{
    config(
        materialized='view'
    )
}}

with tripdata as
(
    select * 
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null 
    and EXTRACT(YEAR FROM pickup_datetime) = 2019
)
select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    pickup_datetime,
    drop_off_datetime as dropoff_datetime,
    {{ dbt.safe_cast('p_ulocation_id', api.Column.translate_type('integer')) }} as pickup_locationid,
    {{ dbt.safe_cast('d_olocation_id', api.Column.translate_type('integer')) }} as dropoff_locationid,
    {{ dbt.safe_cast('sr_flag', api.Column.translate_type('integer')) }} as sr_flag,
    affiliated_base_number
from tripdata
