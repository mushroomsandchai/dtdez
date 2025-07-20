select 
    y.vendor_id,
    y.pickup,
    y.dropoff,
    y.duration,
    y.p_count,
    y.t_distance,
    y.pickup_id,
    zp.zone as pickup_zone,
    y.dropoff_id,
    zd.zone as dropoff_zone,
    y.fare,
    y.tip,
    y.airport_fee,
    y.extras,
    y.total_amount
from 
    {{ ref('stg_yellow') }} y
join 
    {{ ref('stg_zone') }} zp
on 
    zp.id = y.pickup_id
join 
    {{ ref('stg_zone') }} zd
on 
    zd.id = y.dropoff_id