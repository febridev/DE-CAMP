{{ config(materialized='view') }}

with fhv_tripdata as
(
    select *,
        row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging','fhv_trips_data') }}
)

select 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pulocationid,
    cast(dolocationid as integer) as dolocationid,
    cast(sr_flag as integer) as sr_flag
from fhv_tripdata
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}