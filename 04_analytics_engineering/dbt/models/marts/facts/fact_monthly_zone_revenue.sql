select
    coalesce(pickup_zone, 'Unknown zone') as pickup_zone,
    service_type,
    cast(date_trunc(pickup_datetime, month) as date) as revenue_month,
    count(*) as trip_count,
    avg(trip_duration) as avg_trip_duration,
    sum(trip_duration) as total_trip_duration,
    avg(passenger_count) as avg_passenger_count,
    sum(passenger_count) as total_passenger_count,
    avg(trip_distance) as avg_trip_distance,
    sum(trip_distance) as total_trip_distance,
    sum(fare_amount) as fare_amount,
    sum(extra) as extra,
    sum(mta_tax) as mta_tax,
    sum(tip_amount) as tip_amount,
    sum(tolls_amount) as tolls_amount,
    sum(ehail_fee) as ehail_fee,
    sum(improvement_surcharge) as improvement_surcharge,
    sum(total_amount) as total_revenue
from
    {{ ref('fact_trips') }}
group by
    1, 2, 3