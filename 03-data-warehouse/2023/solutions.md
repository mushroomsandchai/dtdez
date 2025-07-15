## Solutions for week 3 - 2023

### Create external from GCP Bucket.

```sql
CREATE OR REPLACE EXTERNAL TABLE `2023.external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtdez_465215_parquet_dump/ny_taxi/fhv/*']
);
```
```sql
CREATE OR REPLACE TABLE `2023.materialized` AS
SELECT *
FROM `2023.external`;
```

### Question 1: What is the count for fhv vehicle records for year 2019?
```sql
SELECT COUNT(*)
FROM `2023.external`;
```
**Answer**: 43244696

### Question 2: Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
```sql
-- Counting distinct base numbers (317.94 MB processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2024.materialized`;
```
```sql
-- Counting distinct base numbers (0 B processed)
SELECT COUNT(DISTINCT `PUlocationID`) AS unq
FROM `2024.external`;
```
**Answer**: 0 MB for the External Table and 317.94 MB for the Materialized Table
internal - 302.55 MB
external - 0B

### Question 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
```sql
SELECT COUNT(*)
FROM `2023.materialized`
WHERE `PUlocationID` IS NULL
  AND `DOlocationID` IS NULL;
```
**Answer**: 717748

### Question 4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
```sql
CREATE OR REPLACE TABLE `2023.pnc`
PARTITION BY DATE(`pickup_datetime`)
CLUSTER BY `affiliated_base_number` AS
SELECT * 
FROM `2023.external`;
```
**Answer**: Partition by pickup_datetime Cluster on affiliated_base_number

### Question 5: Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
```sql
-- Get distinct affiliated_base_number values for March 2019 pickups (23.05 MB processed)
SELECT DISTINCT affiliated_base_number
FROM `2023.pnc`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```
```sql
-- Get distinct affiliated_base_number values for March 2019 pickups (647.87 MB processed)
SELECT DISTINCT(`PULocationID`)
FROM `2024.materialized`
WHERE `tpep_pickup_datetime` BETWEEN '2022-01-06' and '2022-06-30';
```
**Answer**: 647.87 MB for non-partitioned table and 23.05 MB for the partitioned table

### Question 6: Where is the data stored in the External Table you created?
**Answer**: GCP Bucket

### Question 7: It is best practice in Big Query to always cluster your data?
**Answer**: False