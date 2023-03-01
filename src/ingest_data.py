import json
import datetime
import boto3
import logging
import psycopg2
import os

from botocore.exceptions import ClientError

logger = logging.getLogger('TotesysQueryLogger')
logger.setLevel(logging.INFO)

'''
Ingest data from totesys db

Reads updated rows from totesys db and writes to json files in s3.
Writes timestamp.txt file on finish with last updated timestamp.
'''


def get_secret(client):
    '''Retrieves totesys db connection details from aws secrets manager'''

    secret_name = "totesys-db"

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError:
        logger.error('Error retrieving db credentials')
        raise Exception

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)


def db_connect(client):
    '''creates a connection to the totesys database using db credentials from secret stored'''
    db_credentials = get_secret(client)

    host = db_credentials['host']
    username = db_credentials['username']
    password = db_credentials['password']
    database = db_credentials['dbname']
    port = db_credentials['port']

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=username,
        password=password,
        port=port
    )
    return conn


def get_bucket_names(client):
    '''Returns list of bucket names in s3'''
    response = client.list_buckets()
    return [bucket['Name'] for bucket in response['Buckets']]


def write_to_s3(s3, table_name, data, bucket):
    '''Uploads a json object to the specified s3 bucket'''
    try:
        object_path = f'data/{table_name}.json'
        s3.put_object(Body=data, Bucket=bucket, Key=object_path)
        logger.info(f'{table_name} updated')
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'Bucket {bucket} not found')
        else:
            logger.error(f'An error occured writing {table_name}.')
    except Exception:
        logger.error('An error occurred')
        raise Exception
        

def format_data(data, headers):
    '''accepts list of tuples of data and list of tuples equivalent headers, formats as json and returns'''
    list_of_dicts = []
    try:
        for row in data:
            d = {}
            for index, h in enumerate(headers):
                d[h[0]] = row[index]
            list_of_dicts.append(d)
        return json.dumps(list_of_dicts, indent=4, sort_keys=True, default=str)
    except:
        pass

def read_db(query, var_in, cur):
    '''Makes a query to the totesys db'''
    try:
        cur.execute(query, var_in)
        result = cur.fetchall()
        return result
    except Exception as e:
        logger.error(f'An error occured querying {var_in} table.')
        raise e
    
class InvalidTimeStampError(Exception):
    pass

def get_timestamp(s3, ingest_bucket, timestamp_key):
    '''retreives timestamp from bucket - if not in correct format or unable to retrieve raises exception'''
    response = s3.get_object(Bucket=ingest_bucket, Key=timestamp_key)
    timestamp = response['Body'].read().decode("utf-8").rstrip("\n")
    for char in timestamp:
        if char.isalpha() == True or char == ';':
            logger.error('Timestamp file contents unacceptable.')
            raise InvalidTimeStampError
    return timestamp


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

    s3 = boto3.client('s3')
    client = boto3.client('secretsmanager', 'us-east-1')

    try:
        conn = db_connect(client)
    except Exception as e:
        logger.error(e)
        raise
    
    try:
        bucket_list = get_bucket_names(s3)
        bucket_container = {}
        if len(bucket_list) == 0:
            logger.error("No buckets found.")
            return
        else:
            for idx, bucket in enumerate(bucket_list):
                if 'ingested' in bucket:
                    bucket_container['ingested'] = bucket
                if idx == len(bucket_list) - 1 and len(bucket_container) < 1:
                    logger.error('Unable to find ingested bucket.')
        ingested_bucket = bucket_container['ingested']
    except KeyError:
        raise
    except ClientError as c:
        logger.error(c)
        raise
    except Exception as e:
        logger.error(e)
        raise

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
        last_timestamp = get_timestamp(s3, ingested_bucket, 'data/timestamp.txt')
    except InvalidTimeStampError:
        raise
    except Exception:
        last_timestamp = "2022-02-20 09:00:35.185169"
        
    
    last_timestamp_obj = datetime.datetime.strptime(
        last_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    new_timestamp_obj = last_timestamp_obj
    updated_tables = []
    count = 0

    with conn.cursor() as cur:
        for table in tables:
            try:
                updated_rows = read_db(
                    f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,), cur)
                if len(updated_rows) != 0:
                    count += 1
                    updated_tables.append(table)
                    last_updated_data = read_db(
                        f'SELECT last_updated FROM {table} WHERE last_updated > %s ORDER BY last_updated DESC LIMIT 1;', (last_timestamp,), cur)
                    most_recent = last_updated_data[0][0]
                    if most_recent > last_timestamp_obj:
                        new_timestamp_obj = most_recent
            except Exception:
                raise
        for table in updated_tables:
            try:
                file_path = f"{new_timestamp_obj}/{table}"
                updated_rows = read_db(
                    f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,), cur)
                headers = read_db(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (table,), cur)
                data = format_data(updated_rows, headers)
                write_to_s3(s3, file_path, data, ingested_bucket)
                logger.info(f'{table} updated.')
            except Exception:
                raise
        if count > 0:
            try:
                s3.put_object(Body=f'{new_timestamp_obj}',
                            Bucket=ingested_bucket, Key="data/timestamp.txt")
                logger.info('timestamp updated.')
            except:
                logger.info('Error writing timestamp')
        else:
            logger.info('Checked for updates: no updates found')

    conn.close()

    
