import pyarrow.parquet as pq
trips = pq.read_table("/home/kuchoco/data_engineering/1.1_Docker_postgres/yellow_tripdata_2022-01.parquet")
trips = trips.to_pandas()

print(type(trips))
trips.to_csv('/home/kuchoco/data_engineering/1.1_Docker_postgres/trip.csv', index=False)