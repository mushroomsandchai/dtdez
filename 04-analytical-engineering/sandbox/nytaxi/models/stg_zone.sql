select 
    LocationID as id,
    lower(trim(z.Borough)) as borough,
    lower(trim(z.Zone)) as zone,
    lower(trim(z.service_zone)) as service_zone
from 
    {{ ref('zone') }} z