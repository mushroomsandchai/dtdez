# Module 2 Homework: Workflow Orchestration

### Question 1. Within the execution for 'Yellow' Taxi data for the year '2020' and month '12': what is the uncompressed file size (i.e. the output file 'yellow_tripdata_2020-12.csv' of the 'extract' task)?
#### Answer: '134.5 MiB'

### Question 2. What is the rendered value of the variable 'file' when the inputs 'taxi' is set to 'green', 'year' is set to '2020', and 'month' is set to '04' during execution?
#### Answer: green_tripdata_2020-04.csv

### Question 3. How many rows are there for the 'Yellow' Taxi data for all CSV files in the year 2020?
#### Answer: 24,648,499

### Question 4. How many rows are there for the 'Green' Taxi data for all CSV files in the year 2020?
#### Answer: 1,734,051

### Question 5. How many rows are there for the 'Yellow' Taxi data for the March 2021 CSV file?
#### Answer: 1,925,152

### Question 6. How would you configure the timezone to New York in a Schedule trigger?
#### Answer: Add a 'timezone' property set to 'America/New_York' in the 'Schedule' trigger configuration

## References

- [answer to question 1 can be found in the airflow logs, code found here on line 42](./airflow/dags/week2.py)
- [answer to question 2 can be found here on line 28](./airflow/dags/week2.py)
- [answers to questions 3, 4 and 5 can be found here](./bq.sql)
- [answer to question 6 can be found here on line 14](./airflow/dags/week2.py)
- [configuration file](./airflow/docker-compose.yaml)
- [homework questions](./homework.md)