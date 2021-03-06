-- SELECT THE COLUMS INTERESTED FOR YOU
select passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
from `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned` where fare_amount != 0;

-- create a ml table with appropriate type
create or replace table `indigo-anchor-353504.nytaxi.yellow_tripdata_ml` (
  `passenger_count` FLOAT64,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT passenger_count, trip_distance, CAST(PULocationID AS STRING), CAST(DOLocationID AS STRING),
  CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  from `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned` where fare_amount != 0
);

-- create model with default setting
create or replace model `indigo-anchor-353504.nytaxi.tip_model`
OPTIONS
  (model_type = 'linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT * FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;


-- check features
select * from ML.FEATURE_INFO(MODEL `indigo-anchor-353504.nytaxi.tip_model`);


-- EVALUATE THE MODEL
SELECT * FROM
ML.EVALUATE(MODEL `indigo-anchor-353504.nytaxi.tip_model`,
(SELECT * FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL));


-- PREDICT THE MODEL
SELECT * FROM
ML.PREDICT(MODEL `indigo-anchor-353504.nytaxi.tip_model`,
( SELECT * FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL ));

-- PREDICT AND EXPLAIN
SELECT * FROM
ML.EXPLAIN_PREDICT(MODEL `indigo-anchor-353504.nytaxi.tip_model`,
(SELECT * FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL), STRUCT(3 as top_k_features));


-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `indigo-anchor-353504.nytaxi.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT * FROM `indigo-anchor-353504.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;



