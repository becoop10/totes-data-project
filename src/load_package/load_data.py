import psycopg2
import boto3
import pandas as pd
from io import BytesIO , StringIO
from botocore.exceptions import ClientError

import json
import logging
logger = logging.getLogger('WarehouseUploaderLogger')
logger.setLevel(logging.INFO)

def get_secret():
    '''Retrieves data-warehouse db connection details from aws secrets manager'''
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
    '''Build connection to warehouse-db '''
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
    '''Returns list of ingested and processed bucket names regardless of randomised suffix'''
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]

def read_csv(s3, path, bucket):
    '''Reads the contents of a csv object in an s3 bucket and returns as a list of dictionarys'''
    try:
        response = s3.get_object(Bucket=bucket,Key=path)
        file=pd.read_csv(BytesIO(response['Body'].read()))
        r = file.to_dict('records')
        return [k['File names'] for k in r]   
    except s3.exceptions.NoSuchKey as e:
        logger.error(f'Error: No object "{path}" in bucket "{bucket}"')
        raise e
    except ClientError as e:
        logger.error('Error: could not connect to the aws client.')
        raise e
    except:
        logger.error('Error: Could not read csv.')
        raise Exception()

def read_parquets(s3, path, bucket):
    '''Reads the contents of a parquet object in an s3 bucket and returns the contents as a dictionary'''
    try:
        response = s3.get_object(Bucket=bucket,Key=path)
    except ClientError as e:
        logger.error('Error: could not connect to the aws client.')
        raise e
    except s3.exceptions.NoSuchKey as e:
        logger.error(f'Error: No object "{path}" in bucket "{bucket}"')
        raise e
    except:
        logger.error(f'Error.')
        raise Exception()
    try:
        file=pd.read_parquet(BytesIO(response['Body'].read()))
        return file
    except Exception as e:
        logger.error(e)
        logger.error(f'An error occured. Could not read parquet.')
        raise Exception()



def write_to_db(conn, query, var_in):
    '''Writes a sql query to the warehouse db'''
    with conn.cursor() as cur:
        try:
            cur.execute(query, var_in)
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error('An error occured. Could not write to postgres db.')
            logger.error(e)
            logger.error(query)
            raise Exception()


id_columns = {
    'dim_staff' : 'staff_id',
    'dim_counterparty' : 'counterparty_id',
    'dim_currency' : 'currency_id',
    'dim_design' : 'design_id',
    'dim_location' : 'location_id',
    'dim_payment_type' : 'payment_type_id',
    'dim_transaction' : 'transaction_id',
    'fact_sales_order' : 'sales_order_id',
    'fact_purchase_order' : 'purchase_order_id',
    'fact_payment' : 'payment_id'
}

def query_builder(r, filename, invocations):
    '''Builds a SQL query for writing to the warehouse db'''
    keys = list(r.keys())
    values = [r[k] for k in keys]
    update_strings = [f'{k} = EXCLUDED.{k}' for k in keys ]
    fact_update_strings = [ f'{k} = %s' for i, k in enumerate(keys) ]
    full_update_string = ", ".join(update_strings)
    full_fact_update_string = ", ".join(fact_update_strings)
    key_string = ", ".join(keys)

    # if invocations == 0:
    #     var_in = (tuple(values), )
    #     query = f'INSERT INTO {filename} ({key_string}) VALUES %s;'


    if 'dim' in filename:
        var_in = (tuple(values), )
        query = f'INSERT INTO {filename} ({key_string}) VALUES %s ON CONFLICT ({id_columns[filename]}) DO UPDATE SET {full_update_string};'
    else:
        values.append(tuple(values))
        var_in = tuple(values)
        id_v = values[keys.index(id_columns[filename])]
        # query = f'INSERT INTO {filename} ({key_string}) VALUES %s WHERE {id_v} NOT IN (select {id_columns[filename]} FROM {filename});\
        #           UPDATE {filename} SET {full_fact_update_string} WHERE {id_v} IN (select {id_columns[filename]} FROM {filename});'

        query = f'DO $$ declare comparison INT:={id_v}; \
                BEGIN \
                IF comparison IN (select {id_columns[filename]} FROM {filename}) THEN \
                UPDATE {filename} SET {full_fact_update_string} WHERE {id_columns[filename]}={id_v}; \
                ELSE \
                INSERT INTO {filename} \
                ({key_string}) VALUES %s; \
                END IF; \
                END; $$'
                  
    return query, var_in

def data_sorter(data, filename):
    '''Sorts the file data by id'''
    id = id_columns[filename]
    return data.sort_values(id)

def get_file_names(bucket_name,prefix):
    '''Returns a list of file names with a given prefix in an s3 bucket'''
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucket_name,Prefix=prefix)
    try:
        return [file['Key'] for file in response['Contents']]
    except KeyError:
        logger.error("ERROR! No files found")
        return []

def lambda_handler(event, context):
    '''
    Reads parquet files containing inseted/updated rows of each table 
    and writes them to warehouse db.

    Args:
        event:
            Runs on write of updatedfiled.csv in processed bucket
        context:
            a valid AWS lambda Python context object
    Raises:
        Errors result in an informative log message.
  
    '''
    conn = build_connection()
    s3 = boto3.client('s3')
    csv_key = 'updatedfiles.csv'
    bucket = get_bucket_names()[1]
    updated_files = read_csv(s3, csv_key, bucket)

    lam=boto3.client('lambda')

    result=lam.get_function_configuration(
    FunctionName='load-data'
    )
    try:
        response=result['Environment']['Variables']['Invocations']
        logger.info(response)
    except:
        response=0
    response=int(response)

    with conn.cursor() as cur:
        if response==0:

            for key in list(id_columns.keys()).reverse():
                cur.execute(f"DELETE FROM {key};")

        for key in list(id_columns.keys()):
            for f in updated_files:
                if key in f:
                    filename = f.split('/')[1]
                    file_list = sorted(get_file_names(bucket, f'data/{filename}/'))
                    for file in file_list:
                        if file == f'{f}':
                            data = read_parquets(s3, file, bucket)
                            sorted_data = data_sorter(data, filename)
                            response = 0
                            if response == 0:
                                # df = pd.read_parquet('2023-02-24 11_05_10.066000.parquet', engine='fastparquet')
                                # cols = ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']
                                # df = df.reindex(columns=cols)
                                # df['sales_order_id'] = df['sales_order_id'].astype('Int64')
                                # df['purchase_order_id'] = df['purchase_order_id'].astype('Int64')
                                # df['sales_order_id'].replace('', pd.np.nan, inplace = True)
                                # df.to_csv('file_name.csv', index=False)

                                if filename == 'fact_sales_order':
                                    cur.execute(f"ALTER TABLE {filename} ALTER COLUMN sales_record_id RESTART WITH 1;")
                                if filename == 'fact_purchase_order':
                                    cur.execute(f"ALTER TABLE {filename} ALTER COLUMN purchase_record_id RESTART WITH 1;")
                                if filename == 'fact_payment':
                                    cur.execute(f"ALTER TABLE {filename} ALTER COLUMN payment_record_id RESTART WITH 1;")
                                    
                                cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", (filename,))
                                headers=cur.fetchall()
                                headers=[header[0] for header in headers]
                                if filename == "dim_transaction":
                                    cols = ['sales_order_id', 'purchase_order_id']
                                    #sorted_data[cols] = sorted_data[cols].applymap(pd.np.int64)
                                    sorted_data['sales_order_id'] = sorted_data['sales_order_id'].astype('Int64')
                                    sorted_data['purchase_order_id'] = sorted_data['purchase_order_id'].astype('Int64')
                                if 'fact' in filename:
                                    headers.pop(0)
                                sorted_data=sorted_data.replace(pd.np.nan, None)
                                sorted_data = sorted_data.reindex(columns=headers)
                                sorted_data.to_csv('/tmp/data.csv', index=False)
                                logger.info(headers)
                                csv_file_name = '/tmp/data.csv'
                                if 'fact' in filename:
                                    headers_string = ', '.join(headers)
                                    query = f'COPY {filename} ({headers_string}) FROM STDIN DELIMITER \',\' CSV HEADER'
                                    cur.copy_expert(query, open(csv_file_name, "r"))
                                else:
                                    query = f'COPY {filename} FROM STDIN DELIMITER \',\' CSV HEADER'
                                    cur.copy_expert(query, open(csv_file_name, "r"))
                                #cur.copy_from(open(csv_file_name, "r"), filename, columns=headers)
                                conn.commit()
                            else:
                                sorted_data=sorted_data.replace(pd.np.nan, None)
                                sorted_data = sorted_data.to_dict('records')
                                for r in sorted_data:      
                                    query, var_in = query_builder(r, filename, response)
                                    cur.execute(query, var_in)

                            logger.info(f'{f} uploaded to warehouse.')

        conn.commit()
        cur.close()    
    response+=1

    lam.update_function_configuration(
        FunctionName='load-data',
        Environment={
            'Variables':{
                'Invocations':f'{response}'
            }
        }
    )
