#!/usr/bin/env python
# coding: utf-8

# In[1]:
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
# In[3]:

def main(params):

    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    parquet_file=params.parquet_file

    csv_name="yellow_tripdata_2021-01.csv"

    df=pd.read_parquet(parquet_file)
    df.to_csv(csv_name)

    pd.io.sql.get_schema(df,name="yellow_tax_data")

    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    engine=create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    #172.18.0.2
    #engine=create_engine(f"postgresql://{user}:{password}@172.18.0.2:{port}/{db}")


    #print(pd.io.sql.get_schema(df,name="yellow_tax_data", con=engine))

    df_iter=pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df=next(df_iter)

    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")

    while True: 
        t_start=time()

        df=next(df_iter)

        df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end=time()

        print(f'inserted another chunk, took {t_end-t_start} seconds')
    


if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')

    #user, password, host, port

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    #parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--parquet_file', help='parquet file to upload')

    args=parser.parse_args()
    main(args)