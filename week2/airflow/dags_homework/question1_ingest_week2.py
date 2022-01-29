import os

from time import time

import pandas as pd

from sqlalchemy import create_engine

def helloworld():
    print("hello world!!")
    

def ingest_taxi_data(user,password,host,port,db,table_name,csv_file,execution_date):
    print(table_name,csv_file,execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

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
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    # df_zones = pd.read_csv('taxi+_zone_lookup.csv')
    # df_zones.head()