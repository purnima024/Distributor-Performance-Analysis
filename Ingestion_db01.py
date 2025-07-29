import pandas as pd # type: ignore
import os
from sqlalchemy import create_engine # type: ignore
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s -%(message)s",
    filemode="a"
)

engine= create_engine("sqlite:///inventory.db")

def ingest_db(df,tablename,engine):
    # this function() will ingest the dataframe to database table
    df.to_sql(tablename,con=engine,if_exists='replace',index= False)


def load_raw_data():
    # this function() will load the cvs as dataframe and ingest into db
    start= time.time()
    for file in os.listdir('data'):
         if '.csv' in file:
             df= pd.read_csv('data/'+ file)
             logging.info(f"Ingesting {file} in db")
             ingest_db(df,file[:-4],engine)
    
    end= time.time()
    total_time=(end-start)/60
    logging.info("------Ingestion Complete------")
    logging.info(f"\n total time taken {total_time} minutes")         
             
if __name__ =="__main__":
    load_raw_data()