select 
CONCAT(COALESCE(z1."Zone",'Unknown'),'/',COALESCE(z2."Zone",'Unknown')),avg (ytt.total_amount) 
from yellow_taxi_trips ytt 
,zones z1
,zones z2
where ytt."PULocationID" = z1."LocationID" 
and ytt."DOLocationID" = z2."LocationID"
group by CONCAT(COALESCE(z1."Zone",'Unknown'),'/',COALESCE(z2."Zone",'Unknown'))
order by avg (ytt.total_amount) desc