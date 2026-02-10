with vendors as (
    select * from {{ ref('vendor_name_lookup') }}
),
payment_descriptions as (
    select * from {{ ref('payment_type_lookup') }}
),
trips as (
    select * from {{ ref('int_trips_unioned') }}
)
select
    t.trip_id,
    t.vendor_id,
    v.vendor_name as vendor_name,
    t.rate_code_id,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.service_type,
    t.pickup_datetime,
    t.dropoff_datetime,
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
    pd.payment_description as payment_description
from 
    trips t
join 
    vendors v on
    v.vendor_id = t.vendor_id
join 
    payment_descriptions pd on
    pd.payment_type = t.payment_type