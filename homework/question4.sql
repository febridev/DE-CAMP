select MAX(a."tip_amount"), date(a."tpep_pickup_datetime")
from "yellow_taxi_trips" a, "zones" b
where
a."PULocationID" = b."LocationID"
--and a."PULocationID" = 43
--and date(a."tpep_pickup_datetime") = "2021-01-14"
group by date(a."tpep_pickup_datetime")
order by MAX(a."tip_amount") desc