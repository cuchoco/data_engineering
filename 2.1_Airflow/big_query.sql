create or replace external table indigo-anchor-353504.nytaxi.external_yellow_tripdata
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_indigo-anchor-353504/ny_taxi/yellow_tripdata_2019-*.parquet']
);


select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata limit 100;

create or replace table indigo-anchor-353504.nytaxi.external_yellow_tripdata_non_partitioned as 
select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata;


create or replace table indigo-anchor-353504.nytaxi.external_yellow_tripdata_partitioned
partition by DATE(tpep_pickup_datetime) as 
select * from indigo-anchor-353504.nytaxi.external_yellow_tripdata;