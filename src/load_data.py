import psycopg2
import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError
import json
import logging 
logger = logging.getLogger('WarehouseUploaderLogger')
logger.setLevel(logging.INFO)

def get_secret():

    secret_name = "data-warehouse"
    region_name = "us-east-1"

    # Create a Secrets Manager client
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

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def build_connection():
    try:
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
        return conn
    except:
        logger.error('An error occured. Could not establish connection to postgres db.')
        raise Exception()


def get_bucket_names():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]

def read_csv(s3, path, bucket):
    try:
        response = s3.get_object(Bucket=bucket,Key=path)
        file=pd.read_csv(BytesIO(response['Body'].read()))
        r = file.to_dict('records')
        return [k['File names'] for k in r]   
    except:
        logger.error('An error occured. Could not read csv.')
        raise Exception()

def read_parquets(s3, path, bucket):
    try:
        response = s3.get_object(Bucket=bucket,Key=path)
    except:
        logger.error(f'An error occured. Could not get object, Bucket: {bucket}, Path: {path}')
    try:
        file=pd.read_parquet(BytesIO(response['Body'].read()))
        return file.to_dict('records')
    except:
        logger.error(f'An error occured. Could not read parquet. Bucket: {bucket}, Path: {path}')
        raise Exception()



def write_to_db(conn, query, var_in):
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error('An error occured. Could not write to postgres db.')
            raise Exception()


id_columns = {
    'dim_staff' : 'staff_id',
    'dim_date' : 'date_id',
    'fact_sales_order' : 'sales_record_id',
    'dim_counterparty' : 'counterparty_id',
    'dim_currency' : 'currency_id',
    'dim_design' : 'design_id',
    'dim_location' : 'location_id',
    'fact_sales_order' : 'sales_record_id',
    'dim_payment_type' : 'payment_type_id',
    'fact_payment' : 'payment_record_id',
    'dim_transaction' : 'transaction_id'
}

def query_builder(r, filename):
    keys = r.keys()
    values = [r[k] for k in keys]
    update_strings = [f'{k} = EXCLUDED.{k}' for k in keys ]
    full_update_string = ", ".join(update_strings)
    var_in = (keys, values)
    query = f'INSERT INTO {filename} (%s) VALUES (%s) ON CONFLICT ({id_columns[filename]}) DO UPDATE SET {full_update_string};'
    return query, var_in

def data_sorter(data, filename):
    id = id_columns[filename]
    return sorted(data, key=lambda a: a[id] )

def lambda_handler(event, context):
    conn = build_connection()
    s3 = boto3.client('s3')
    csv_key = 'updatedfiles.csv'
    bucket = get_bucket_names()[1]
    updated_files = read_csv(s3, csv_key, bucket)

    for f in updated_files:
        filename = f.split('/')[1]
        data = read_parquets(s3, f'{f}', bucket)
        sorted_data = data_sorter(data, filename)
        for r in sorted_data:      
            query, var_in = query_builder(r, filename)
            write_to_db(conn, query, var_in)
        logger.info(f'{f} uploaded to warehouse.')


'''
set up aws secret and credentials
test data is being uploaded
'''
        