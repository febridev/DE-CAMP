select a."DOLocationID", b."Zone", c."Zone", count(a."DOLocationID"), date(a."tpep_pickup_datetime")
from "yellow_taxi_trips" a, "zones" b, "zones" c
where
a."PULocationID" = b."LocationID"
and a."DOLocationID" = c."LocationID"
and b."Zone" = 'Central Park'
and date(a."tpep_pickup_datetime") = '2021-01-14'
group by a."DOLocationID",b."Zone", c."Zone", date(a."tpep_pickup_datetime")
order by count(a."DOLocationID") desc