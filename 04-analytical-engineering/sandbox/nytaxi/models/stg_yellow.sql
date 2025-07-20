select 
    VendorID as vendor_id,
    tpep_pickup_datetime as pickup,
    tpep_dropoff_datetime as dropoff,
    timestamp_diff(tpep_dropoff_datetime, tpep_pickup_datetime, second) as duration,
    cast(passenger_count as int64) as p_count,
    trip_distance as t_distance,
    PULocationID as pickup_id,
    DOLocationID as dropoff_id,
    fare_amount as fare,
    coalesce(tip_amount, 0) as tip,
    coalesce(airport_fee, 0) as airport_fee,
    round(total_amount - 
        coalesce(tip_amount, 0) - 
        fare_amount - 
        coalesce(airport_fee, 0),
        2) as extras,
    total_amount
from 
    {{ source('ny_taxi', 'yellow_2018_07') }}
where 
    fare_amount >= 0 and
    passenger_count is not null and
    tpep_pickup_datetime < tpep_dropoff_datetime

{{ is_test_run(test) }}