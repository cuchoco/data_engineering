#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import os
import pyarrow.parquet as pq

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'
    # download the parquet and conver to csv
    os.system(f"wget {url} -O {csv_name}")
    file = pq.read_table(csv_name)
    file = file.to_pandas()
    file.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{user}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

   
    while True:
        t_start = time()
        
        try:
            df = next(df_iter)
        except StopIteration:
            print("Ingesting job finished")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


        


def Parser():
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # user, passwd, host
    # port
    # database name
    # table name
    # url of the csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='passwd for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = Parser()
    main(args)








