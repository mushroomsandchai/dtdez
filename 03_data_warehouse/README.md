# Module 3 Homework: Data Warehouse

### 1. What is count of records for the 2024 Yellow Taxi Data?
#### Answer: 20,332,093

```sql

create or replace external table `project_id.dataset.week3_ext` options(
  uris = ['gs://homework_dtdez/week3/*.parquet'],
  format = 'parquet'
);
select count(*) from `project_id.dataset.week3_ext`;
```


### 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
#### Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

```sql
create or replace table `project_id.dataset.week3` as
select * from `project_id.dataset.week3_ext`;

select count(distinct PULocationID) from `project_id.dataset.week3_ext`;
select count(distinct PULocationID) from `project_id.dataset.week3`;
```


### 3. Why are the estimated number of Bytes different? Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
#### Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

```sql
select PULocationID from `project_id.dataset.week3`;
select PULocationID, DOLocationID from `project_id.dataset.week3`;
```


### 4. How many records have a fare_amount of 0?
#### Answer: 8,333

```sql
select count(*) from `project_id.dataset.week3`
where fare_amount = 0;
```


### 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
#### Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
create or replace table `project_id.dataset.week3_pc`
partition by date(tpep_dropoff_datetime)
cluster by VendorID as
select * from `project_id.dataset.week3`;
```


### 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
#### Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```sql
select distinct VendorID from `project_id.dataset.week3_pc`
where 
    date(tpep_dropoff_datetime) >= '2024-03-01' and
    date(tpep_dropoff_datetime) <= '2024-03-15';

select distinct VendorID from `project_id.dataset.week3`
where 
    date(tpep_dropoff_datetime) >= '2024-03-01' and
    date(tpep_dropoff_datetime) <= '2024-03-15';
```


### 7. Where is the data stored in the External Table you created? 
#### Answer: GCP Bucket


### 8. It is best practice in Big Query to always cluster your data?
#### Answer: No


### 9. Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
#### Answer: Since big query caches some metadata when a table is materialized, it simply outputs the cached data instead of reading the number of records all over again.

```sql
select count(*) from `project_id.dataset.week3`;
```

## References

- [ingestion dag](./airflow/dags/week3.py)
- [configuration file](./airflow/docker-compose.yaml)
- [homework questions](./homework.md)
- [airflow initialization script](./airflow/init_airflow.sh)
