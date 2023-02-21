from myutils import get_bucket_names,get_file_names,get_file_contents,write_file_to_processed_bucket,format_counterparty,format_currency,format_design,format_staff,format_location,format_transaction,format_payment_type, format_payments,format_sales_facts,format_purchase

import boto3
import json
import logging
import pandas as pd
from io import BytesIO

logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)
import pyarrow



def lambda_handler(event, context):
    s3=boto3.client('s3')
    
    try:
        bucket_name_list = get_bucket_names()
    
        ingested_bucket = bucket_name_list[0]
        processed_bucket = bucket_name_list[1]
    except:
        logger.error("ERROR! No buckets found")
        raise Exception("NO BUCKETS TO RETRIEVE DATA")


    file_list = get_file_names(ingested_bucket,f'data/{timestamp}/')

    timestamp=s3.get_object(Bucket=ingested_bucket,Key="data/timestamp.txt")['Body'].read().decode('utf-8')

    tablenames=["sales_order","counterparty","currency","department","design","staff","address","payment_type","payment","purchase_order","transaction"]
    dataToBeFormatted={}
    updatedfiles=[]
    
    addressfile=""
    departfile=""
    for file in file_list:
        if "address" in file:
            addressfile=file
        if "department" in file:
            departfile=file
        for table in tablenames:
            if file==f'data/{timestamp}/{table}.json':
                dataToBeFormatted[f'{table}_data']=get_file_contents(ingested_bucket,file)
                if table=="counterparty" and f'data/{timestamp}/address' not in file_list:
                
                    dataToBeFormatted['address_data']=get_file_contents(ingested_bucket,addressfile)
                if table=="staff" and f'data/{timestamp}/department' not in file_list:
                    
                    dataToBeFormatted['department_data']=get_file_contents(ingested_bucket,departfile)
                tablenames.remove(table)
                
                


    
    
    try:
        formatted_counterparty = format_counterparty(dataToBeFormatted['counterparty_data'],dataToBeFormatted['address_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_counterparty/{timestamp}.parquet', formatted_counterparty)
        logger.info("dim_counterparty parquet updated")
        updatedfiles.append(f'data/dim_counterparty/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Counterparty has no new data")
 
    try:
        formatted_currency = format_currency(dataToBeFormatted['currency_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_currency/{timestamp}.parquet', formatted_currency)
        logger.info("dim_currency parquet updated")
        updatedfiles.append(f'data/dim_currency/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Currency has no new data")

    try:
        formatted_transaction = format_transaction(dataToBeFormatted['transaction_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_transaction/{timestamp}.parquet', formatted_transaction)
        logger.info("dim_transaction parquet updated")
        updatedfiles.append(f'data/dim_transaction/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Transaction has no new data")

    try:
        formatted_design=format_design(dataToBeFormatted['design_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_design/{timestamp}.parquet', formatted_design)
        logger.info("dim_design parquet updated")
        updatedfiles.append(f'data/dim_design/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Design has no new data")

    try:
        formatted_payment_type=format_payment_type(dataToBeFormatted['payment_type_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_payment_type/{timestamp}.parquet', formatted_payment_type)
        logger.info("dim_payment parquet updated")
        updatedfiles.append(f'data/dim_payment_type/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Payment type has no new data")

    try:
        formatted_payments=format_payments(dataToBeFormatted['payment_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/fact_payment/{timestamp}.parquet', formatted_payments)
        logger.info("fact_payment parquet updated")
        updatedfiles.append(f'data/fact_payment/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Payments have no new data")

    try:
        formatted_purchase=format_purchase(dataToBeFormatted['purchase_order_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/fact_purchase_order/{timestamp}.parquet', formatted_purchase)
        logger.info("fact_purchase parquet updated")
        updatedfiles.append(f'data/fact_purchase_order/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Purchase has no new data")

    try:
        formatted_sales = format_sales_facts(dataToBeFormatted['sales_order_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/fact_sales_order/{timestamp}.parquet', formatted_sales)
        logger.info("fact_sales parquet updated")
        updatedfiles.append(f'data/fact_sales_order/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Sales has no new data")

    try:
        formatted_staff=format_staff(dataToBeFormatted['staff_data'],dataToBeFormatted['department_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_staff/{timestamp}.parquet', formatted_staff)
        logger.info("dim_staff parquet updated")
        updatedfiles.append(f'data/dim_staff/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Staff has no new data")

    try:       
        formatted_location=format_location(dataToBeFormatted['address_data'])
        write_file_to_processed_bucket(
        processed_bucket, f'data/dim_location/{timestamp}.parquet', formatted_location)
        logger.info("dim_location parquet updated")
        updatedfiles.append(f'data/dim_location/{timestamp}.parquet')
    except KeyError as error:
        logger.info("Location has no new data")
    
    s3.put_object(Body=updatedfiles, Bucket=processed_bucket, Key="updatedfiles.txt")
 