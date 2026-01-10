# Module 1 Homework: Docker & SQL

### Question 1. Run docker with the `python:3.13` image. What's the version of `pip` in the image?
#### Answer: 25.3
```console
ajay@dtdez:~$ docker run -it --entrypoint bash python:3.13
root@6f0f93275cde:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

### Question 2. Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?
#### Answer: db:5432, postgres:5432
##### Best Practice to use service_name:internally_exposed_port(db:5432)


### Question 3. For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?
#### Answer: 8,007
```sql
select 
    count(*) as num_trips
from green
where 
    lpep_pickup_datetime >= '2025-11-01' and 
    lpep_pickup_datetime < '2025-12-01' and
    trip_distance <= 1
```
+-----------+
| num_trips |
|-----------|
| 8007      |
+-----------+


### Question 4. Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors). Use the pick up time for your calculations.

#### Answer: 2025-11-14
```sql
select date(lpep_pickup_datetime) as date
from green
where trip_distance = (
                        select max(trip_distance) from green
                        where trip_distance < 100
                        )
```
+------------+
| date       |
|------------|
| 2025-11-14 |
+------------+


### Question 5. Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?
#### Answer: East Harlem North
```sql
select 
    z."Zone", 
    round(sum(g.total_amount)::numeric, 2) as total_amount
from green g
join "zone" z on z."LocationID" = g."PULocationID"
where date(g.lpep_pickup_datetime) = '2025-11-18'
group by z."Zone"
order by 2 desc
limit 5
```
+--------------------------+--------------+
| Zone                     | total_amount |
|--------------------------+--------------|
| East Harlem North        | 9281.92      |
| East Harlem South        | 6696.13      |
| Central Park             | 2378.79      |
| Washington Heights South | 2139.05      |
| Morningside Heights      | 2100.59      |
+--------------------------+--------------+

### Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
#### Answer: Yorkville West
```sql
select
    z2."Zone",
    max(tip_amount) as max_tip
from green g
join "zone" z1 on z1."LocationID" = g."PULocationID"
join "zone" z2 on z2."LocationID" = g."DOLocationID"
where 
    z1."Zone" = 'East Harlem North' and 
    extract(month from g.lpep_pickup_datetime) = 11
group by z2."Zone"
order by 2 desc
limit 5
```
+-------------------------------+---------+
| Zone                          | max_tip |
|-------------------------------+---------|
| Yorkville West                | 81.89   |
| LaGuardia Airport             | 50.0    |
| East Harlem North             | 45.0    |
| Long Island City/Queens Plaza | 34.25   |
| <null>                        | 28.9    |
+-------------------------------+---------+


### Question 7. Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

#### Answer: terraform init, terraform apply -auto-approve, terraform destroy

## References

- [notebook](./solution.ipynb)
- [configuration file](./docker-compose.yaml)
- [homework questions](./homework.md)