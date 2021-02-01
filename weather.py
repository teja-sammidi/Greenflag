
"""
 Module for converting weather data from csv to parquet and upload o S3

"""

import pandas as pd
import os
import sys
import fastparquet
import boto3
import boto
from botocore.exceptions import ClientError
from boto.exception import S3CreateError
#import filepath
from os import getenv

#bucket_name=sys.argv[1]
#print(bucket_name)


def read_source_files():
    try:
        df1 = pd.read_csv(getenv('file_path') + 'weather.20160201.csv')
        df2 = pd.read_csv(getenv('file_path') + 'weather.20160301.csv')
        df_weather = pd.DataFrame()
        df_weather = df_weather.append([df1, df2],ignore_index=True)
        if df_weather.empty:
            raise TypeError('Dataframe has no data.Verify source files')
        #check if any columns has missing values
        #df_weather.isna().sum()
        df_weather[['WindGust', 'Visibility', 'Pressure']] = df_weather[['WindGust', 'Visibility', 'Pressure']].fillna(0)
        df_weather['Country'] = df_weather['Country'].fillna('')
        df_weather['ObservationDate'] = pd.to_datetime(df_weather['ObservationDate'])
        return df_weather
    except IOError as error :
        print(" Failed Operation " , error.strerror)

def convert_to_parquet():
    try:
        df_weather = read_source_files() 
        df_weather.to_parquet('df_weather_pq.parquet.gzip',compression='gzip')
        #df_weather_pq = pd.read_parquet('df_weather_pq.parquet.gzip')
    except Exception as error :
        print(error)
        

  
       
def create_s3_bucket(bucket_name):
   try:
       boto_kwargs = {
    "aws_access_key_id": getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": getenv("AWS_REGION"),
    }
       s3_client = boto3.Session(**boto_kwargs).client("s3")
       name=bucket_name
       s3_client.create_bucket(Bucket=name,CreateBucketConfiguration={'LocationConstraint': getenv("AWS_REGION")})
       upload_parquet_to_s3(sys.argv[1])
   except Exception:
      print("bucket name already exists, please use a unique name")
               
               
        
#s3 = boto3.client('s3')
#s3.create_bucket(Bucket='weather-bucket',CreateBucketConfiguration={'LocationConstraint': 'us-east-1'})

def upload_parquet_to_s3(bucket_name):
   
   try:
        s3 = boto3.resource('s3')
        name=bucket_name
        s3.meta.client.upload_file(getenv('file_path') + 'df_weather_pq.parquet.gzip',name,'df_weather_pq.parquet.gzip')
        print("file uploaded")
   except Exception as error :
       print(error)
        
        
if __name__ == '__main__':
    read_source_files()
    convert_to_parquet()
    create_s3_bucket(sys.argv[1])
    

    

    

        
        
        
        
    
        

