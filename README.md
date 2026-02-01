# de-charles
# Data Engineering zoomcamp
# Module 1

Question 3:
select COUNT(*) 
from green_tripdata 
where lpep_pickup_datetime>='2025-11-01' 
and lpep_pickup_datetime<'2025-12-01'
and trip_distance<=1

Question 4:

select 
    lpep_pickup_datetime,
    trip_distance,
    date(lpep_pickup_datetime)
from green_tripdata
where trip_distance < 100
order by trip_distance desc

Question 5:

after creating a Python file with "nano filename.py", load amongst others Pandas library to create dataframes. Then, run sql in docker and do a left join. Then group by to sum over our column of interest:

select 
    tz."Zone",
    SUM(gt.total_amount) as total_amount_sum
from green_tripdata gt
join taxi_zones tz on gt."PULocationID" = tz."LocationID"
where date(gt.lpep_pickup_datetime) = '2025-11-18'
group by tz."Zone"
order by total_amount_sum desc

Question 6: 

select
    tz_dropoff."Zone" as dropoff_zone,
    MAX(gt.tip_amount) as largest_tip
from green_tripdata gt
join taxi_zones tz_pickup on gt."PULocationID" = tz_pickup."LocationID"
join taxi_zones tz_dropoff on gt."DOLocationID" = tz_dropoff."LocationID"
where tz_pickup."Zone" = 'East Harlem North'
and gt.lpep_pickup_datetime >= '2025-11-01'
and gt.lpep_pickup_datetime < '2025-12-01'
group by tz_dropoff."Zone"
group by largest_tip desc


# Module 2

Question 1: 
docker exec -it mach9103-kestra-1 find /app/storage -type f -name "*yellow_tripdata_2020-12.csv*" -exec ls -lh {} \;
Result: 129M

Question 2: 
variables:
  file: "{{ inputs.taxi }}_tripdata_{{ inputs.year }}-{{ inputs.month }}.csv"
green_tripdata_2020-04.csv

Question 3: 
Create a new kestra flow that disregards "month" and includes a countRows task. See the file "04_postgres_taxi_all_months"
Result: 24648511 total

Question 4: 
Same as above. Result: 1734063 total

Question 5: 
Include 2021 as a year in the existing kestra flow: 
-- inputs:
  - id: taxi
    type: SELECT
    displayName: Select taxi type
    values: [yellow, green]
    defaults: yellow

  - id: year
    type: SELECT
    displayName: Select year
    values: ["2019", "2020", "2021"]
    defaults: "2019"

  - id: month
    type: SELECT
    displayName: Select month
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    defaults: "01"

Result: 

$ docker exec -it mach9103-pgdatabase-1 psql -U root -d ny_taxi -c "SELECT COUNT(*) FROM public.yellow_tripdata WHERE filename = 'yellow_tripdata_2021-03.csv';"
  count
---------
 1925152
(1 row)

Question 6:

