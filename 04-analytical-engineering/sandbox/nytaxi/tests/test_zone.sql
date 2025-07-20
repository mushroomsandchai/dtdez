select
    *
from
    {{ ref('stg_zone' )}}
where  
    id = 0