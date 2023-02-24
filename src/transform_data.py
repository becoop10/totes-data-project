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

    timestamp=s3.get_object(Bucket=ingested_bucket,Key="data/timestamp.txt")['Body'].read().decode('utf-8')


    file_list = get_file_names(ingested_bucket,f'data/{timestamp}/')


    ingestedTableNames=["sales_order","counterparty","currency","department","design","staff","address","payment_type","payment","purchase_order","transaction"]

    dataToBeFormatted={}
    updatedfiles=[]
    
    addressfile=""
    departfile=""
    for file in file_list:
        if "address" in file:
            addressfile=file
        if "department" in file:
            departfile=file
        for table in ingestedTableNames:
            if file==f'data/{timestamp}/{table}.json':
                dataToBeFormatted[f'{table}_data']=get_file_contents(ingested_bucket,file)
                if table=="counterparty" and f'data/{timestamp}/address' not in file_list:
                
                    dataToBeFormatted['address_data']=get_file_contents(ingested_bucket,addressfile)
                if table=="staff" and f'data/{timestamp}/department' not in file_list:
                    
                    dataToBeFormatted['department_data']=get_file_contents(ingested_bucket,departfile)
                ingestedTableNames.remove(table)


    processTableNames={"dim_counterparty":['counterparty_data','address_data',format_counterparty],
                        "dim_currency":['currency_data',format_currency],
                        "dim_transaction":['transaction_data',format_transaction],
                        "dim_design":['design_data',format_design],
                        "dim_payment_type":['payment_type_data',format_payment_type],
                        "fact_payment":['payment_data',format_payments],
                        "fact_purchase_order":["purchase_order_data",format_purchase],
                        "fact_sales_order":["sales_order_data",format_sales_facts],
                        "dim_staff":["staff_data","department_data",format_staff],
                        "dim_location":["address_data",format_location]
    }


    for datakey in dataToBeFormatted.keys():
        for tablekey in processTableNames.keys():
            if datakey in processTableNames[tablekey]:
                functionList=processTableNames[tablekey]
                if tablekey=="dim_counterparty":
                    formatteddata=functionList[2](dataToBeFormatted[functionList[0]],dataToBeFormatted[functionList[1]])
                elif tablekey=="dim_staff":
                    formatteddata=functionList[2](dataToBeFormatted[functionList[0]],dataToBeFormatted[functionList[1]])
                else: 
                    formatteddata=functionList[1](dataToBeFormatted[functionList[0]])
                try:
                    filestring=f'data/{tablekey}/{timestamp}.parquet'
                    write_file_to_processed_bucket(processed_bucket,filestring,formatteddata)
                    logger.info(f'{tablekey} parquet updated')
                    if filestring not in updatedfiles:
                        updatedfiles.append(filestring)
                        df=pd.DataFrame(updatedfiles[-1],columns=["File names"])
                        out_buffer=BytesIO()
                        df.to_csv(out_buffer,index=False)
                        s3.put_object(Body=out_buffer.getvalue(), Bucket=processed_bucket, Key="updatedfiles.csv")

                except Exception as e:
                    logger.error(e)
    
    