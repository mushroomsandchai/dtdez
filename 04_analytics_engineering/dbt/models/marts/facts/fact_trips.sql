with trips as (
    select
        *
    from
        {{ ref('dim_payment_vendor_joined') }}
),
zones as (
    select 
        * 
    from 
        {{ ref('dim_zones') }}
)
select
    t.trip_id,
    t.vendor_id,
    t.vendor_name,
    t.rate_code_id,
    t.pickup_location_id,
    pz.zone as pickup_zone,
    pz.borough as pickup_borough,
    t.dropoff_location_id,
    dz.zone as dropoff_zone,
    dz.borough as dropoff_borough,
    t.service_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    timestamp_diff(t.pickup_datetime, t.dropoff_datetime, minute) as trip_duration,
    t.store_and_fwd_flag,
    t.passenger_count,
    t.trip_distance,
    t.trip_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type,
    t.payment_description
from
    trips t
join
    zones pz on
    pz.location_id = t.pickup_location_id
join
    zones dz on
    dz.location_id = t.dropoff_location_id