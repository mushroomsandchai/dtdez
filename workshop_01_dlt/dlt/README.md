
## nytaxi.py

Pipeline that loads NYC taxi trip data from the Data Engineering Zoomcamp API into DuckDB using [dlt](https://dlthub.com/). Data is fetched in pages of 1000 records and appended to the destination.

**Usage**

```bash
python nytaxi.py
```

**Requirements**

```bash
dlt
requests
```
Defaults: destination DuckDB, dataset name `dlt`. 

Inspect(duckdb needs to be installed) the loaded data in DuckDB with 
```sql
select * from dlt.nytaxi;
```