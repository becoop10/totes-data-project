import json
import datetime
import boto3

import logging 
import psycopg2
import os

from botocore.exceptions import ClientError

'''
Ingest data from totesys db

Reads updated rows from totesys db and writes to json files in s3.
Writes timestamp.txt file on finish with last updated timestamp.
'''

def get_secret():
    '''Retrieves totesys db connection details from aws secrets manager'''

    secret_name = "totesys-db"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

db = get_secret()
host = db['host']
username = db['username']
password = db['password']
database = db['dbname']
port = db['port']

conn = psycopg2.connect(
    host = host,
    database = database,
    user = username,
    password = password,
    port = port
)
 
def get_file_contents(bucket_name, file_name):
    '''Returns body of s3 object'''
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        return response['Body'].read()

def get_bucket_names():
    '''Returns list of ingested and processed bucket names regardless of randomised suffix'''
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
    '''Uploads a json object to the specified s3 bucket'''
    try:
        object_path = f'data/{table_name}.json'
        s3.put_object(Body=data, Bucket=bucket, Key=object_path)
    except ClientError as e:
        logger.error('A Client Error occured.')
        raise e 
    except:
        logger.error('An error occured.')
        raise Exception()

def format_data(data, headers):
    '''accepts list of tuples of data and list of tuples equivalent headers, formats as json and returns'''
    list_of_dicts = []
    for row in data:
        d = {}
        for index, h in enumerate(headers):
            d[h[0]] = row[index]
        list_of_dicts.append(d)

    return json.dumps(list_of_dicts, indent=4, sort_keys=True, default=str)


def read_db(query, var_in):
    '''Makes a query to the totesys db'''
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            result = cur.fetchall()
            cur.close()
            return result
        except:
            logger.error('An error occured.')
            raise Exception()


# def write_timestamp():
#     try:
#         current_timestamp = datetime.datetime.now()
#         os.environ['totesys_last_read'] = current_timestamp.isoformat()
#     except Exception as e:
#         logger.error('An error occured.')
#         raise Exception()

# def retrieve_timestamp():
#     try:
#         if 'totesys_last_read' in os.environ:
#            return os.environ['totesys_last_read']
#         else:
#             return datetime.datetime(1970,1,1).isoformat()
#     except:
#         logger.error('An error occured.')
#         raise Exception()

def lambda_handler(event, context):
    '''
    Reads last run timestamp from timestamp.txt and uses this to
    query totesys db to retrieve recent rows only (will query entire
    tables on first run). Writes updates to ingested s3 bucket and
    overwrites timestamp.txt with current timestamp.

    Args:
        event:
            Scheduled event every x minutes, takes no input.
        context:
            a valid AWS lambda Python context object
    Raises:
        Errors result in an informative log message.
  
    '''
    try:
        bucket_name_list=get_bucket_names()
        bucket_name=bucket_name_list[0]
    except ClientError as e:
        logger.error('Error connecting to aws.')
        raise e
    except IndexError as e:
        logger.error('Error:Buckets not set up correctly.')
        raise e
    except:
        logger.error('An Error occurred.')
        raise Exception()

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
        try:
            byte_timestamp = get_file_contents(bucket_name, 'data/timestamp.txt')
        except:
            byte_timestamp=b'2000-01-01 00:00:00.000000'

        last_timestamp = byte_timestamp.decode("utf-8").rstrip("\n")
        last_timestamp_obj = datetime.datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        new_timestamp = last_timestamp_obj
        updated_tables = []
        count=0
        for table in tables:
            updated_rows = read_db(f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,))
            if len(updated_rows) != 0:
                count += 1
                updated_tables.append(table)
                last_updated_data = read_db(f'SELECT last_updated FROM {table} WHERE last_updated > %s ORDER BY last_updated DESC LIMIT 1;', (last_timestamp,))
                most_recent = last_updated_data[0][0]
                if most_recent > last_timestamp_obj:
                    new_timestamp = most_recent
        for table in updated_tables:
            file_path = f"{new_timestamp}/{table}"
            updated_rows = read_db(f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,))
            headers = read_db("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (table,))
            data = format_data(updated_rows, headers)
            write_to_s3(s3, file_path, data, bucket_name)
            logger.info(f'{table} table updated.')
        if count > 0:
            logger.info(f'{count}')
            s3.put_object(Body=f'{new_timestamp}', Bucket=bucket_name, Key="data/timestamp.txt")

