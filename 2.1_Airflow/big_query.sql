create or replace external table indigo-anchor-353504.nytaxi.external_yellow_tripdata
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_indigo-anchor-353504/ny_taxi/yellow_tripdata_2019-*.parquet']
);


select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata limit 100;


/*
  Partitioning
*/

create or replace table indigo-anchor-353504.nytaxi.yellow_tripdata_non_partitioned as 
select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata;

create or replace table indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned
partition by DATE(tpep_pickup_datetime) as 
select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata;


-- This query will process 106.37 MB when run.
select distinct(VendorID)
from `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned` 
where date(tpep_pickup_datetime) between '2019-06-01' and '2019-06-30';

-- This query will process 1.26 GB when run.
select distinct(VendorID)
from `indigo-anchor-353504.nytaxi.yellow_tripdata_non_partitioned` 
where date(tpep_pickup_datetime) between '2019-06-01' and '2019-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

/*
  Clustering
*/

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM  `indigo-anchor-353504.nytaxi.external_yellow_tripdata`


-- Query scans 715.8 MB
SELECT count(*) as trips
FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-12-31'
AND VendorID=2;

-- Query scans 640.97 MB 
SELECT count(*) as trips
FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-12-31'
AND VendorID=2;

