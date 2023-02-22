import pyarrow
import boto3
import json
import logging
import pandas as pd
from io import BytesIO
from star_schema.src.utils.utils import format_counterparty, format_transaction, format_currency, format_design, format_location, format_payment_type, format_payments, format_purchase, format_sales_facts, format_staff
logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)


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
    s3 = boto3.client('s3')

    try:
        bucket_name_list = get_bucket_names()

        ingested_bucket = bucket_name_list[0]
        processed_bucket = bucket_name_list[1]
    except:
        logger.error("ERROR! No buckets found")

    file_list = get_file_names(ingested_bucket)

    timestamp = s3.get_object(
        Bucket=ingested_bucket, Key="data/timestamp.txt")['Body'].read().decode('utf-8')

    for file in file_list:
        if file == f'data/{timestamp}/sales_order.json':
            sales_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/counterparty.json':
            counterparty_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/currency.json':
            currency_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/department.json':
            department_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/design.json':
            design_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/staff.json':
            staff_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/address.json':
            address_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/payment_type.json':
            payment_type_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/payment.json':
            payment_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/purchase_order.json':
            purchase_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/transaction.json':
            transaction_data = get_file_contents(ingested_bucket, file)
        if file == f'data/{timestamp}/location.json':
            location_data = get_file_contents(ingested_bucket, file)

    try:
        formatted_counterparty = format_counterparty(
            counterparty_data, address_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_counterparty.parquet', formatted_counterparty)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_currency = format_currency(currency_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_currency.parquet', formatted_currency)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_transaction = format_transaction(transaction_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_transaction.parquet', formatted_transaction)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_design = format_design(design_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_design.parquet', formatted_design)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_payment_type = format_payment_type(payment_type_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_payment_type.parquet', formatted_payment_type)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_payments = format_payments(payment_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/fact_payment.parquet', formatted_payments)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_purchase = format_purchase(purchase_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/fact_purchase_order.parquet', formatted_purchase)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_sales = format_sales_facts(sales_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/fact_sales_order.parquet', formatted_sales)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_staff = format_staff(staff_data, department_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_staff.parquet', formatted_staff)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")

    try:
        formatted_location = format_location(location_data)
        write_file_to_processed_bucket(
            processed_bucket, 'data/dim_location.parquet', formatted_location)
    except UnboundLocalError as error:
        logger.info(error.args[0].split(" ")[2], "Table not found")


lambda_handler('hello', 'world')
