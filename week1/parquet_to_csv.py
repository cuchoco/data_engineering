import pyarrow.parquet as pq
trips = pq.read_table("/home/cuchoco/data_engineering/week1/yellow_tripdata_2022-01.parquet")
trips = trips.to_pandas()

print(type(trips))
trips.to_csv('/home/cuchoco/data_engineering/week1/trip.csv', index=False)