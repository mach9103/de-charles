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
129M

Question 2: 
variables:
  file: "{{ inputs.taxi }}_tripdata_{{ inputs.year }}-{{ inputs.month }}.csv"
green_tripdata_2020-04.csv

Question 3: 
Create a new kestra flow that disregards "month" and includes a countRows task. See the 

