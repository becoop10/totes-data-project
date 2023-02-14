import psycopg2
import json
import datetime
import boto3
import aws
import logging 

'''
s3 -store last timestamp somewhere

Read from totesys - Can we limit to new data only?
    timestamp, length of table, ids
convert to required format - json?
log progress to Cloudwatch
trigger email alerts in the event of failures
upload to s3 - append new data
'''

host = ''
username = ''
password = ''
database = ''
port = 0

conn = psycopg2.connect(
    host = host,
    database = database,
    user = username,
    password = password,
    port = port
)

def write_to_s3(s3, table_name, data, bucket):
    try:
        object_path = f'data/{table_name}.json'
        s3.put_object(Body=data, Bucket=bucket, Key=object_path)
    except:
        raise Exception()

def format_data(data):
    return json.dumps(data)
    pass


def read_db(query, var_in):
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            result = cur.fetchall()
            cur.close()
            return result
        except:
            raise Exception()

def log_progress():
    pass


def lambda_handler(event, context):
    with conn:         
        ####S3 CONNECTION
        s3 = ''
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
        last_timestamp = datetime.datetime(2022, 12, 12) #access from s3
        for table in tables:
            query = f'SELECT * FROM {table} WHERE last_updated >= %s;'
            result = read_db(query, (last_timestamp))
            data = format_data(result)
            write_to_s3(s3, table, data, 'totes-amazeballs-s3-ingested-data-bucket' )
        
        current_timestamp = datetime.datetime.now()
    #write timestamp to aws env var

