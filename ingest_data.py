#!/usr/bin/env python
# coding: utf-8

import argparse, os
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd

def main(params):
  user = params.user
  password= params.password
  host=params.host
  port=params.port
  db=params.db
  table_name=params.table_name
  url=params.url

  csv_name = 'output.csv'
  os.system(f"wget {url} -O {csv_name}")

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  engine.connect()

  df_iter = pd.read_csv('./desktop/yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)


  while True:
    df=next(df_iter)
    df = df[df['tpep_pickup_datetime'] != 'tpep_pickup_datetime']
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
    df.to_sql(name=table_name, con=engine, if_exists='append', dtype={
    'VendorID': sqlalchemy.types.BigInteger(),
    'tpep_pickup_datetime': sqlalchemy.types.DateTime(),
    'tpep_dropoff_datetime': sqlalchemy.types.DateTime(),
    'passenger_count': sqlalchemy.types.Float(),
    'trip_distance': sqlalchemy.types.Float(),
    'RatecodeID': sqlalchemy.types.Float(),
    'store_and_fwd_flag': sqlalchemy.types.Text(),
    'PULocationID': sqlalchemy.types.BigInteger(),
    'DOLocationID': sqlalchemy.types.BigInteger(),
    'payment_type': sqlalchemy.types.BigInteger(),
    'fare_amount': sqlalchemy.types.Float(),
    'extra': sqlalchemy.types.Float(),
    'mta_tax': sqlalchemy.types.Float(),
    'tip_amount': sqlalchemy.types.Float(),
    'tolls_amount': sqlalchemy.types.Float(),
    'improvement_surcharge': sqlalchemy.types.Float(),
    'total_amount': sqlalchemy.types.Float(),
    'congestion_surcharge': sqlalchemy.types.Float(),
    'airport_fee': sqlalchemy.types.Float()
})
    


# In[ ]:




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table-name', help='table name to write the results')
    parser.add_argument('url', help='url of the csv file')
    args=parser.parse_args()
    main(args)


  



