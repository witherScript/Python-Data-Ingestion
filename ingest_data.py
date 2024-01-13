#!/usr/bin/env python
# coding: utf-8

import argparse, os
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

def main(params):
  user = params.user
  password= params.password
  host=params.host
  port=params.port
  db=params.db
  table_name=params.table_name
  url=params.url

  parquet_out = 'output.parquet'
  os.system(f"wget {url} -O {parquet_out}")

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  engine.connect()
  df = pd.read_parquet(parquet_out)
  df.to_sql(name='yellow_taxi', con=engine, if_exists='replace')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table-name', help='table name to write the results')
    parser.add_argument('--url', help='url of the csv file')
    args=parser.parse_args()
    main(args)


  



