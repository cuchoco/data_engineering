-- SELECT THE SOLUMS INTERESTED FOR YOU
select passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
from `indigo-anchor-353504.nytaxi.yellow_tripdata_partitioned` where fare_amount != 0;

