# Workshop #1 dlt

### Question 1: What is the start date and end date of the dataset?
#### Answer: 2009-06-01 to 2009-07-01
```sql
select 
    min(nytaxi.Trip_pickup_Date_time::date) as start_date, 
    max(nytaxi.trip_dropoff_date_time::date) as end_date 
from dlt.nytaxi;
```

### Question 2: What proportion of trips are paid with credit card?
#### Answer: 26.66%
```sql
with total_count as (
    select count(*) from dlt.nytaxi
)
select 
    nytaxi.payment_type, 
    (count(*) * 100) / (select * from total_count) as percentage 
from dlt.nytaxi 
group by 1 
order by 2 desc;
```

### Question 3: What is the total amount of money generated in tips?
#### Answer: $6,063.41
```sql
select 
    round(sum(nytaxi.tip_amt), 2) as total_tip_amount 
from dlt.nytaxi;
```

## References
- [dlt pipeline](./dlt/nytaxi.py)
- [homework](./homework.md)