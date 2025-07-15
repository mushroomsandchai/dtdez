## Solutions for week 3 - 2024

### Create external and materialized tables from GCP Bucket.

```sql
CREATE OR REPLACE EXTERNAL TABLE `2024.external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtdez_465215_parquet_dump/ny_taxi/green/*']
);
```
```sql
CREATE OR REPLACE TABLE `2024.materialized` AS
SELECT *
FROM `2024.external`;
```

### Question 1: What is count of records for the 2022 Green Taxi Data?
```sql
SELECT COUNT(*)
FROM `2024.external`;
```
**Answer**: 39656098

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
```sql
-- Selecting only PUlocationID (302.55 MB processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2024.materialized`;
```
```sql
-- Selecting only PUlocationID (0 MB processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2024.external`;
```
**Answer**: 0 MB for the External Table and 302.55 MB for the Materialized Table
internal - 302.55 MB
external - 0B

### Question 3: How many records have a fare_amount of 0?
```sql
SELECT COUNT(fare_amount)
FROM `2024.external`
WHERE fare_amount = 0;
```
**Answer**: 17274

### Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
```sql
CREATE OR REPLACE TABLE `2024.pnc`
PARTITION BY DATE(`tpep_pickup_datetime`)
CLUSTER BY `PUlocationID` AS
SELECT * 
FROM `2024.external`;
```
**Answer**: Partition by tpep_pickup_datetime Cluster on PUlocationID

### Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 
```sql
SELECT DISTINCT(`PULocationID`)
FROM `2024.materialized`
WHERE `tpep_pickup_datetime` BETWEEN '2022-01-06' AND '2022-06-30';
```
```sql
SELECT DISTINCT(`PULocationID`)
FROM `2024.pnc`
WHERE `tpep_pickup_datetime` BETWEEN '2022-01-06' and '2022-06-30';
```
**Answer**: 605.1 MB for non-partitioned table and 297.14 for the partitioned table

### Question 6: Where is the data stored in the External Table you created?
**Answer**: GCP Bucket

### Question 7: It is best practice in Big Query to always cluster your data?
**Answer**: False

### Question 8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
**Answer**: 0 Bytes because the number of rows are stored in the metadata.  