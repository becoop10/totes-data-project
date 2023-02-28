import json
import datetime
import boto3

import logging
import psycopg2

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
    '''creates a connection to the totesys database using db
    credentials from secret stored'''
    db_credentials = get_secret(client)

    host = db_credentials['host']
    username = db_credentials['username']
    password = db_credentials['password']
    database = db_credentials['dbname']
    port = db_credentials['port']

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=port
        )
        return conn
    except Exception:
        logger.error('Error connecting to db')


def get_bucket_names():
    '''Returns list of ingested and processed bucket names regardless
    of randomised suffix'''
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]


def write_to_s3(s3, table_name, data, bucket):
    '''Uploads a json object to the specified s3 bucket'''
    try:
        object_path = f'data/{table_name}.json'
        s3.put_object(Body=data, Bucket=bucket, Key=object_path)
        logger.info(f'{table_name} updated')
    except BaseException:
        logger.error(f'An error occured writing {table_name}.')


def format_data(data, headers):
    '''accepts list of tuples of data and list of tuples equivalent headers,
      formats as json and returns'''
    list_of_dicts = []
    for row in data:
        d = {}
        for index, h in enumerate(headers):
            d[h[0]] = row[index]
        list_of_dicts.append(d)

    return json.dumps(list_of_dicts, indent=4, sort_keys=True, default=str)


def read_db(query, var_in, db_conn):
    '''Makes a query to the totesys db'''
    cur = db_conn.cursor()
    try:
        cur.execute(query, var_in)
        result = cur.fetchall()
        cur.close()
        return result
    except BaseException:
        logger.error(f'An error occured querying {var_in} table.')


def get_timestamp(s3, ingest_bucket, timestamp_key):
    '''retreives timestamp from bucket - if not in correct format or
    unable to retrieve raises exception'''
    try:
        response = s3.get_object(Bucket=ingest_bucket, Key=timestamp_key)
        timestamp = response['Body'].read().decode("utf-8").rstrip("\n")
        for char in timestamp:
            if char.isalpha() or char == ';':
                logger.error('Timestamp file contents unacceptable')
                raise Exception()
        return timestamp
    except BaseException:
        logger.error('Unable to retrieve timestamp.')
        raise Exception()


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
    conn = db_connect(client)

    try:
        bucket_name_list = get_bucket_names()
        ingested_bucket = bucket_name_list[0]
    except ClientError as e:
        logger.error('Error connecting to aws.')
        raise e
    except IndexError as e:
        logger.error('Error:Buckets not set up correctly.')
        raise e
    except BaseException:
        logger.error('An Error occurred.')
        raise Exception()

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

    last_timestamp = get_timestamp(s3, ingested_bucket, 'data/timestamp.txt')
    last_timestamp_obj = datetime.datetime.strptime(
        last_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    new_timestamp_obj = last_timestamp_obj
    updated_tables = []
    count = 0

    for table in tables:
        updated_rows = read_db(
            f'''SELECT *
                FROM {table}
                WHERE last_updated > %s;''', (last_timestamp,), conn)
        if len(updated_rows) != 0:
            count += 1
            updated_tables.append(table)
            last_updated_data = read_db(
                f'''SELECT last_updated
                    FROM {table}
                    WHERE last_updated > %s
                    ORDER BY last_updated
                    DESC LIMIT 1;''',
                (last_timestamp,
                 ),
                conn)
            most_recent = last_updated_data[0][0]
            if most_recent > last_timestamp_obj:
                new_timestamp_obj = most_recent
    for table in updated_tables:
        file_path = f"{new_timestamp_obj}/{table}"
        updated_rows = read_db(
            f'''SELECT *
                FROM {table}
                WHERE last_updated > %s;''', (last_timestamp,), conn)
        headers = read_db(
             '''SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s;''', (table, ), conn)
        data = format_data(updated_rows, headers)
        write_to_s3(s3, file_path, data, ingested_bucket)
        logger.info(f'{table} updated.')
    if count > 0:
        s3.put_object(Body=f'{new_timestamp_obj}',
                      Bucket=ingested_bucket, Key="data/timestamp.txt")
        logger.info('timestamp updated.')
    else:
        logger.info('Checked for updates: no updates found')

    conn.close()

    # for table in tables:
    #     updated_rows = read_db(
    #         f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,))
    #     if len(updated_rows) != 0:
    #         count += 1
    #         updated_tables.append(table)
    #         last_updated_data = read_db(
    #             f'SELECT last_updated FROM {table} WHERE last_updated > %s ORDER BY last_updated DESC LIMIT 1;', (last_timestamp,))
    #         most_recent = last_updated_data[0][0]
    #         if most_recent > last_timestamp_obj:
    #             new_timestamp_obj = most_recent
    # for table in updated_tables:
    #     file_path = f"{new_timestamp_obj}/{table}"
    #     updated_rows = read_db(
    #         f'SELECT * FROM {table} WHERE last_updated > %s;', (last_timestamp,))
    #     headers = read_db(
    #         "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (table,))
    #     data = format_data(updated_rows, headers)
    #     write_to_s3(s3, file_path, data, ingest_bucket)
    #     logger.info(f'{table} table updated.')
    # if count > 0:
    #     logger.info(f'{count}')
    #     s3.put_object(Body=f'{new_timestamp_obj}',
    #                     Bucket=ingest_bucket, Key="data/timestamp.txt")
