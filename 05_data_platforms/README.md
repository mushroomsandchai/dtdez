# Module 5 Homework: Data Platforms(bruin)

### Question 1. In a Bruin project, what are the required files/directories?
#### Answer: .bruin.yml and pipeline/ with pipeline.yml and assets/


### Question 2. You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?
#### Answer: time_interval - incremental based on a time column


### Question 3. You have a variable defined in pipeline.yml:variables: taxi_types: type: array items: type: string default: ["yellow", "green"]How do you override this when running the pipeline to only process yellow taxis?
#### Answer: bruin run --var 'taxi_types=["yellow"]'


### Question 4. You've modified the ingestion/trips.py asset and want to run it plus all downstream assets. Which command should you use?
#### Answer: bruin run ingestion/trips.py --downstream


### Question 5. You want to ensure the pickup_datetime column in your trips table never has NULL values. Which quality check should you add to your asset definition?
#### Answer: name: not_null


### Question 6. After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?
#### Answer: bruin lineage


### Question 7. You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch? 
#### Answer: --full-refresh


## References

- [pipeline](./nytaxi/pipeline.yml)
- [homework questions](./homework.md)


##### Note: This project assumes you have created three dataset(dev, homework, prod) and loaded your external tables to dev dataset. Since I've gone about creating my own models, macros to answer this weeks homework, column names, table names and queries will differ from that in [official datatalks dbt project repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/taxi_rides_ny).
