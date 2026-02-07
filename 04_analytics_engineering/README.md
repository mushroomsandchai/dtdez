# Module 4 Homework: Analytical Engineering

### Question 1. ```dbt run --select int_trips_unioned``` builds which models?
#### Answer: int_trips_unioned only
##### If we were to run ```dbt run --select +int_trips_unioned``` all the upstream dependencies including int_trips_unioned would run.<br/><br/>Likewise, if we ran ```dbt run --select int_trips_unioned+``` all the downstream dependencies would run including int_trips_unioned.



### Question 2. New value 6 appears in payment_type. What happens on dbt test?
#### Answer: dbt will fail the test, returning a non-zero exit code



### Question 3. What is the count of records in the fct_monthly_zone_revenue model?
#### Answer: 12,184

```sql
with grouped as (
    select 
        count(1) 
    from 
        `madowd.dtdez.fact_trip` 
    group by 
        date_trunc(pickup_timestamp, month), 
        pickup_zone, 
        service_type
)
select count(*) from grouped;
```


### Question 4. Best Performing Zone for Green Taxis (2020). Which zone had the highest revenue?
#### Answer: East Harlem North

```sql
select 
    pickup_zone, 
    sum(total_revenue) as total_revenue 
from 
    `madowd.dtdez.fact_monthly_revenue` 
where 
    year = 2020 and 
    service_type = 'green' 
group by 1
order by 2 desc;
```


### Question 5. Total trips for Green taxis in October 2019?
#### Answer: 384,624

```sql
select 
    sum(trip_count) as total_trips
from 
    `madowd.dtdez.fact_monthly_revenue` 
where 
    year = 2019 and 
    month = 10 and 
    service_type = 'green';
```

### Question 6. Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?
#### Answer: 43,244,696

```sql
select 
    count(*) as total_count 
from 
    `madowd.dtdez.stg_fhv`;
```

##### Note: Choosing the closest answer, since 43,244,693 is the actual count and there are 3 null values.

## References

- [configuration file](./airflow/docker-compose.yaml)
- [homework questions](./homework.md)


##### Note: Since I've gone about creating my own models, macros to answer this weeks homework, column names, table names and queries will differ from that in [official datatalks dbt project repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/taxi_rides_ny).