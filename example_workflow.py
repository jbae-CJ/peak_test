from sqlalchemy import text, create_engine
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from sklearn.linear_model import LinearRegression
import boto3

import os


## This function connects you to Redshift
def connect_to_redshift():
    connection_str = 'postgresql://{usr}:{pwd}@{host}:5439/{db}'.format(
                        usr=os.environ['REDSHIFT_USERNAME'],
                        pwd=os.environ['REDSHIFT_PASSWORD'],
                        host=os.environ['REDSHIFT_HOST'],
                        db=os.environ['TENANT']
                    )
    sql_engine = create_engine(connection_str)
    return sql_engine


def read_from_redshift(sql_engine, query):
    with sql_engine.connect() as conn:
        data = conn.execute(query)
        df = pd.DataFrame(list(data), columns=list(data.keys()))
    return df


## Function to save data to S3
def write_file_to_s3(bucket, filepath, filename):
    """
    EXAMPLE 
    Choose a suitable bucket name, filepath name and file name:

    bucket = 'prod-cjaicenter-datalake-f56'
    filepath = 'cjaicenter/datascience'
    filename = 'houseprice_predictions.csv'
    """
    s3 = boto3.resource('s3')
    key = '{}/{}'.format(filepath, filename)
    s3.meta.client.upload_file(filename, bucket, key)


bucket = 'prod-cjaicenter-datalake-f56'
filepath = 'cjaicenter/datascience'
filename = 'houseprices_predictions.csv'

## Query to select data from Redshift
query = """
select * from stage.houseprices
"""

sql_engine = connect_to_redshift()
df = read_from_redshift(sql_engine, query)


## Removing categorical features
df = df[df['price'] > 0]
data = df.drop(['date','street', 'city', 'statezip', 'country', 'peakauditcreatedat', 'peakauditupdatedat', 
                'peakauditupdatecounter'], axis=1)

## Build Model and add predictions to dataset
model = LinearRegression().fit(data.drop('price', axis=1), data['price'])

predictions = model.predict(data.drop('price', axis=1))
df['predictions'] = predictions

## Changing datatypes as table in Redshift has integers
df['price'] = df['price'].astype(int)
df['predictions'] = df['predictions'].astype(int)
df['bedrooms'] =  df['bedrooms'].astype(int)
df['bathrooms'] =  df['bathrooms'].astype(int)
df['floors'] =  df['floors'].astype(int)
df = df.drop(['peakauditcreatedat', 'peakauditupdatedat', 'peakauditupdatecounter'], axis=1)


df.to_csv(filename, index=False)

write_file_to_s3(bucket, filepath, filename)

## Querys to create, delete and copy predictions from S3
create_tabele_query = """
CREATE TABLE IF NOT EXISTS publish.houseprice_predictions (
date date encode zstd,
  price integer encode zstd,
  bedrooms integer encode zstd,
  bathrooms integer encode zstd,
  sqftliving integer encode zstd,
  sqftlot integer encode zstd,
  floors integer encode zstd,
  waterfront integer encode zstd,
  view integer encode zstd,
  condition integer encode zstd,
  sqftabove integer encode zstd,
  sqftbasement integer encode zstd,
  yrbuilt integer encode zstd,
  yrrenovated integer encode zstd,
  street varchar(256) encode zstd,
  city varchar(256) encode zstd,
  statezip varchar(256) encode zstd,
  country varchar(256) encode zstd,
  predictions integer encode zstd
)
"""

delete_query = "delete from publish.houseprice_predictions"

copy_query = """copy publish.houseprice_predictions
from 's3://{}/{}/{}' 
IAM_ROLE 'arn:aws:iam::794236216820:role/RedshiftS3Access'
CSV DELIMITER ',' 
IGNOREHEADER 1""".format(bucket, filepath, filename)


#sql_engine.execute(text(create_query).execution_options(autocommit=True))
sql_engine.execute(text(create_tabele_query).execution_options(autocommit=True))

sql_engine.execute(text(delete_query).execution_options(autocommit=True))

sql_engine.execute(text(copy_query).execution_options(autocommit=True))


