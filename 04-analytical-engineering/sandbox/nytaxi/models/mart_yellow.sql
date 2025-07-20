select 
    count(*) as no_of_trips,
    round(avg(duration), 2) as duration,
    pickup_zone,
    dropoff_zone,
    round(avg(fare), 2) as average_fare,
    round(avg(t_distance), 2) as average_trip_distance
from 
    {{ ref('int_yellow') }}
group by 
    pickup_zone,
    dropoff_zone
order by 
    no_of_trips desc

{{ is_test_run(test) }}