with yellow as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        "yellow" as service_type,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        cast(1 as integer) as trip_type, -- yellow taxi only does street-hail.
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        cast(0 as integer) as ehail_fee, -- no ehail fee as they only do street-hail.
        improvement_surcharge,
        total_amount,
        payment_type
    from 
        {{ ref('stg_yellow_tripdata') }}
),
green as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        "green" as service_type,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type
    from
     {{ ref('stg_green_tripdata') }}
),
unioned as (
    select * from yellow
    union all 
    select * from green
)

select 
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 
        'pickup_datetime', 
        'dropoff_datetime',
        'pickup_location_id', 
        'dropoff_location_id',
        'service_type']) }} as trip_id,
    * 
from 
    unioned

-- data quality check. if there are any records with pickup date not in 2019 and 2020, we should filter them out.
-- additionaly, this filter only applies to production environment since the homework repo doesn't use this filter.
{% if target.name == 'prod' %}
    where pickup_datetime >= '2019-01-01' and pickup_datetime <= '2020-12-31'
{% endif %}
qualify
    row_number() over(partition by vendor_id, pickup_datetime, pickup_location_id, service_type) = 1