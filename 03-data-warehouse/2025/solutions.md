## Solutions for week 3 - 2025

### Create external and materialized tables from GCP Bucket.

```sql
CREATE OR REPLACE EXTERNAL TABLE `2025.external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtdez_465215_parquet_dump/ny_taxi/yellow/*']
);
```
```sql
CREATE OR REPLACE TABLE `2025.materialized` AS
SELECT *
FROM `2025.external`;
```

### Question 1: What is count of records for the 2024 Yellow Taxi Data?
```sql
SELECT COUNT(fare_amount)
FROM `2025.external`;
```
**Answer**: 20332093

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
```sql
-- Selecting only PUlocationID (155.12 MB processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2025.materialized`;
```
```sql
-- Selecting only PUlocationID (0 MB processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2025.external`;
```
**Answer**: 0 MB for the External Table and 155.12 MB for the Materialized Table
internal - 155.12MB
external - 0B

### Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
```sql
-- Selecting only PUlocationID (155.12 MB processed)
SELECT PUlocationID
FROM `2025.materialized`;
```
```sql
-- Selecting both PUlocationID and DOlocationID (310.24 MB processed)
SELECT PUlocationID, DOlocationID
FROM `2025.materialized`;
```
**Answer**: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

### Question 4: How many records have a fare_amount of 0?
```sql
SELECT COUNT(fare_amount)
FROM `2025.external`
WHERE fare_amount = 0;
```
**Answer**: 8333

### Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
```sql
CREATE OR REPLACE TABLE `2025.pnc`
PARTITION BY DATE(`tpep_dropoff_datetime`)
CLUSTER BY `VendorID` AS
SELECT *
FROM `2025.external`;
```
**Answer**: Partition by tpep_dropoff_datetime and Cluster on VendorID

### Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
**Answer**: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
```sql
-- Selecting from non-partitioned table (310.24 MB processed)
SELECT COUNT(DISTINCT `VendorID`)
FROM `2025.materialized`
WHERE DATE(`tpep_dropoff_datetime`) BETWEEN '2024-03-01' AND '2024-03-15';
```
```sql
-- Selecting from partitioned table (26.84 MB processed)
SELECT COUNT(DISTINCT `VendorID`)
FROM `2025.pnc`
WHERE DATE(`tpep_dropoff_datetime`) BETWEEN '2024-03-01' AND '2024-03-15';
```

### Question 7: Where is the data stored in the External Table you created?
**Answer**: GCP Bucket

### Question 8: It is best practice in Big Query to always cluster your data?
**Answer**: No

### Question 9: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
**Answer**: 0 Bytes because the number of rows are stored in the metadata.  