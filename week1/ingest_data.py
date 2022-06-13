#!/usr/bin/env python
# coding: utf-8

from time import time
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


df_iter = pd.read_csv('/home/cuchoco/data_engineering/week1/trip.csv', iterator=True, chunksize=100000)

df = next(df_iter)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


while True:
    t_start = time()
    
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()
    print('inserted another chunk..., took %.3f second' % (t_end - t_start))





