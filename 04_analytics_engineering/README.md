# Module 4 Homework: Analytical Engineering

### Question 1. ```dbt run --select int_trips_unioned``` builds which models?
#### Answer: int_trips_unioned only
##### If we were to run ```dbt run --select +int_trips_unioned``` all the upstream dependencies including int_trips_unioned would run.<br/><br/>Likewise, if we ran ```dbt run --select int_trips_unioned+``` all the downstream dependencies would run including int_trips_unioned.



### Question 2. New value 6 appears in payment_type. What happens on dbt test?
#### Answer: dbt will fail the test, returning a non-zero exit code

##### The following sql script creates the necessary tables for this weeks homework
```sql
create or replace external table `project_id.dev.green_tripdata_external` options(
  uris = ['gs://bucket/green/*'],
  format = 'csv'
);

create or replace external table `project_id.dev.yellow_tripdata_external` options(
  uris = ['gs://bucket/yellow/*'],
  format = 'csv'
);

create or replace external table `project_id.dev.fhv_tripdata_external` options(
  uris = ['gs://bucket/fhv/*'],
  format = 'csv'
);
```


### Question 3. What is the count of records in the fct_monthly_zone_revenue model?
#### Answer: 12,184

```sql
select 
    count(*) as total_records
from 
    `project_id.homework.fact_monthly_zone_revenue`;
```


### Question 4. Best Performing Zone for Green Taxis (2020). Which zone had the highest revenue?
#### Answer: East Harlem North

```sql
select 
    pickup_zone, 
    sum(total_revenue) as total_zone_revenue
from 
    `project_id.homework.fact_monthly_zone_revenue`
where 
    service_type = 'green' and 
    extract(year from revenue_month) = 2020
group by 1
order by 2 desc
limit 5;
```


### Question 5. Total trips for Green taxis in October 2019?
#### Answer: 384,624

```sql
select 
    sum(trip_count) as total_trips
from 
    `project_id.homework.fact_monthly_zone_revenue`
where 
    service_type = 'green' and 
    extract(year from revenue_month) = 2019 and 
    extract(month from revenue_month) = 10;
```

### Question 6. Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?
#### Answer: 43,244,693

```sql
select 
    count(*) as total_count 
from 
    `project_id.dataset.fhv_tripdata_external`
where 
    dispatching_base_num is not null;
```


## References

- [configuration file](./airflow/docker-compose.yaml)
- [homework questions](./homework.md)


##### Note: This project assumes you have created three dataset(dev, homework, prod) and loaded your external tables to dev dataset. Since I've gone about creating my own models, macros to answer this weeks homework, column names, table names and queries will differ from that in [official datatalks dbt project repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/taxi_rides_ny).