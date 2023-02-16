import boto3
import json

import pandas as pd
from io import BytesIO
from star_schema.src.utils.utils import format_counterparty, format_currency, format_design, format_location, format_payment_type, format_payments, format_purchase, format_sales_facts, format_staff


def get_bucket_names():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]


def get_file_names(bucket_name):
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucket_name)
    return [file['Key'] for file in response['Contents']]


def get_file_contents(bucket_name, file_name):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    jsonfile = json.loads(response['Body'].read())
    return jsonfile


def write_file_to_processed_bucket(bucket_name, key, list):
    s3 = boto3.client('s3')
    pandadataframe = pd.DataFrame(list)
    out_buffer = BytesIO()
    pandadataframe.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=key, Body=out_buffer.getvalue())


def lambda_handler(event, context):
    bucket_name_list = get_bucket_names()
    ingested_bucket = bucket_name_list[0]
    processed_bucket = bucket_name_list[1]
    file_list = get_file_names(ingested_bucket)
    for file in file_list:
        if "sales_order" in file:
            sales_data = get_file_contents(ingested_bucket, file)
            formatted_sales = format_sales_facts(sales_data)
            write_file_to_processed_bucket(
                processed_bucket, 'data/fact_sales_order.parquet', formatted_sales)
            


lambda_handler('hello', 'world')
