import json
import datetime
import boto3

import logging 
import psycopg2
import os
import botocore.exceptions
'''
s3 -store last timestamp somewhere
Read from totesys - Can we limit to new data only?
    timestamp, length of table, ids
convert to required format - json?
log progress to Cloudwatch
trigger email alerts in the event of failures
upload to s3 - append new data
'''

host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
username = 'project_user_3'
password = 'EbD7qkwt5xWYUwnx2nbhdvkC'
database = 'totesys'
port = 5432

conn = psycopg2.connect(
    host = host,
    database = database,
    user = username,
    password = password,
    port = port
)
 

def get_bucket_names():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]


logger = logging.getLogger('TotesysQueryLogger')
logger.setLevel(logging.INFO)

def write_to_s3(s3, table_name, data, bucket):
    '''
    Test for errors, handle special exceptions
    '''
    try:
        object_path = f'data/{table_name}.json'
        s3.put_object(Body=data, Bucket=bucket, Key=object_path)
    except:
        logger.error('An error occured.')
        raise Exception()

def format_data(data, headers):
    '''
    accepts list of tuples of data and list of tuples equivalent headers, formats as json and returns
    '''
    list_of_dicts = []
    for row in data:
        d = {}
        for index, h in enumerate(headers):
            d[h[0]] = row[index]
        list_of_dicts.append(d)

    return json.dumps(list_of_dicts, indent=4, sort_keys=True, default=str)


def read_db(query, var_in):
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            result = cur.fetchall()
            cur.close()
            return result
        except:
            logger.error('An error occured.')
            raise Exception()


def write_timestamp():
    try:
        current_timestamp = datetime.datetime.now()
        os.environ['totesys_last_read'] = current_timestamp.isoformat()
    except Exception as e:
        logger.error('An error occured.')
        raise Exception()

def retrieve_timestamp():
    try:
        if 'totesys_last_read' in os.environ:
           return os.environ['totesys_last_read']
        else:
            return datetime.datetime(1970,1,1).isoformat()
    except:
        logger.error('An error occured.')
        raise Exception()

def lambda_handler(event, context):
    bucket_name_list=get_bucket_names()
    bucket_name=bucket_name_list[0]

    with conn:         
        s3 = boto3.client('s3')
        tables = [
            'counterparty',
            'currency',
            'department',
            'design',
            'staff',
            'sales_order',
            'address',
            'payment',
            'purchase_order',
            'payment_type',
            'transaction'   
        ]
        current_timestamp = datetime.datetime.now()
        try:
            last_timestamp = f'{s3.get_object(Bucket=bucket_name,Key="data/timestamp.txt")["Body"].read().decode("utf-8")}'
        except:
            last_timestamp=f'{datetime.datetime(1970,1,1).isoformat()}'
        write_timestamp()
        count = 0
        for table in tables:
            updates = read_db(f'SELECT * FROM {table} WHERE last_updated >= %s;', (last_timestamp, ))
            if len(updates) > 0:
                count+=1
                query = f'SELECT * FROM {table};'
                result = read_db(query, ())
                headers = read_db("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (table, ))
                data = format_data(result, headers)

                tablestring=f"{current_timestamp}/{table}"
                write_to_s3(s3, tablestring, data, bucket_name )
                logger.info(f'{table} table updated.')
        if count > 0:
            s3.put_object(Body=f'{current_timestamp}', Bucket=bucket_name, Key="data/timestamp.txt")

            s3.put_object(Body=f'{count}', Bucket=bucket_name, Key="data/count.txt")
        # write_timestamp()
