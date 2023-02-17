import json
import datetime
import boto3
import logging 
import psycopg2
import os
import botocore.exceptions


host = os.environ['totesys_host']
username = os.environ['totesys_username']
password = os.environ['totesys_password']
database = os.environ['totesys_database']
port = os.environ['totesys_port']

conn = psycopg2.connect(
    host = host,
    database = database,
    user = username,
    password = password,
    port = port
)

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

def read_from_s3(s3,bucket, key):
    try:
        content = s3.get_object(Bucket=bucket, Key=key)
        content = content['Body'].read().decode('ascii')
        return content
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(e)
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
        last_timestamp = retrieve_timestamp()
        write_timestamp()
        for table in tables:
            #Check for updates
            updates = read_db(f'SELECT * FROM {table} WHERE last_updated >= %s;', (last_timestamp,))
            if len(updates) > 0:
                #Reads table data
                result = read_db(f'SELECT * FROM {table};', ())
                headers = read_db("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (table,))
                #format data
                data = format_data(result, headers)
                #write to s3 bucket
                write_to_s3(s3, table, data, 'totes-amazeballs-s3-ingested-data-bucket-12345' )
                logger.info(f'{table} table updated.')


