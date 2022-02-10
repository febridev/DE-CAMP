CREATE OR REPLACE TABLE portfoliode-demo.trips_data_all.fhv_trips_data_partition_clustered
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM portfoliode-demo.trips_data_all.fhv_trips_data;



SELECT count(*) FROM portfoliode-demo.trips_data_all.fhv_trips_data_partition_clustered
WHERE DATE(dropoff_datetime) >= "2019-01-01" 
and DATE(dropoff_datetime) < "2019-03-31"
and dispatching_base_num in ('B00987', 'B02060', 'B02279')