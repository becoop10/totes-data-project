if __name__ == "__main__":
    from myutils import (
        get_bucket_names, get_file_names, get_file_contents,
        write_file_to_processed_bucket, format_counterparty,
        format_currency, format_design, format_staff,
        format_location, format_transaction, format_payment_type,
        format_payments, format_sales_facts, format_purchase
    )
    import json
    import pyarrow


else:
    from function.myutils import (
        get_bucket_names, get_file_names, get_file_contents,
        write_file_to_processed_bucket, format_counterparty,
        format_currency, format_design, format_staff,
        format_location, format_transaction, format_payment_type,
        format_payments, format_sales_facts, format_purchase
    )

import boto3
import logging
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError

logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    '''
    Reads data from the ingested bucket and formats into
    the star schema for data warehouse, then writes
    to the processed s3 bucket as parquet files.
    Writes updatedfiles.csv to trigger load lambda.

    Args:
        event:
            Runs on write of timestamp.txt
        context:
            a valid AWS lambda Python context object
    Raises:
        Errors result in an informative log message.

    '''
    s3 = boto3.client('s3')

    bucket_list = get_bucket_names()
    for bucket in bucket_list:
        if 'ingested' in bucket:
            ingested_bucket = bucket
        elif 'processed' in bucket:
            processed_bucket = bucket
        else:
            logger.error("Error retrieving bucket names")
            raise Exception

    try:
        timestamp = s3.get_object(
            Bucket=ingested_bucket,
            Key="data/timestamp.txt")['Body'].read().decode('utf-8')
    except s3.exceptions.NoSuchKey as e:
        logger.error(
            f'Error: No object "timestamp.txt" in bucket "{ingested_bucket}"')
        raise e
    except ClientError as e:
        logger.error('Error: could not connect to the aws client.')
        raise e
    except Exception:
        logger.error('Error: Could not read txt.')
        raise Exception

    file_list = get_file_names(ingested_bucket, f'data/{timestamp}/')

    ingestedTableNames = ["sales_order", "counterparty", "currency",
                          "department", "design", "staff", "address",
                          "payment_type", "payment", "purchase_order",
                          "transaction"]

    dataToBeFormatted = {}
    updatedfiles = []

    addressfile = ""
    departfile = ""

    for file in file_list:
        if "address" in file:
            addressfile = file
        if "department" in file:
            departfile = file
        for table in ingestedTableNames:
            if file == f'data/{timestamp}/{table}.json':
                dataToBeFormatted[f'{table}_data'] = get_file_contents(
                    ingested_bucket, file)
                if (table == "counterparty"
                        and f'data/{timestamp}/address' not in file_list):
                    dataToBeFormatted['address_data'] = get_file_contents(
                        ingested_bucket, addressfile)
                if (table == "staff"
                        and f'data/{timestamp}/department' not in file_list):

                    dataToBeFormatted['department_data'] = get_file_contents(
                        ingested_bucket, departfile)
                ingestedTableNames.remove(table)

    processTableNames = {
        "dim_counterparty":
        ['counterparty_data', 'address_data', format_counterparty],
        "dim_currency": ['currency_data', format_currency],
        "dim_transaction": ['transaction_data', format_transaction],
        "dim_design": ['design_data', format_design],
        "dim_payment_type": ['payment_type_data', format_payment_type],
        "fact_payment": ['payment_data', format_payments],
        "fact_purchase_order": ["purchase_order_data", format_purchase],
        "fact_sales_order": ["sales_order_data", format_sales_facts],
        "dim_staff": ["staff_data", "department_data", format_staff],
        "dim_location": ["address_data", format_location]
    }

    for datakey in dataToBeFormatted.keys():
        for tablekey in processTableNames.keys():
            if datakey in processTableNames[tablekey]:
                functionList = processTableNames[tablekey]
                if tablekey == "dim_counterparty":
                    formatteddata = functionList[2](
                        dataToBeFormatted[functionList[0]],
                        dataToBeFormatted[functionList[1]])
                elif tablekey == "dim_staff":
                    formatteddata = functionList[2](
                        dataToBeFormatted[functionList[0]],
                        dataToBeFormatted[functionList[1]])
                else:
                    formatteddata = functionList[1](
                        dataToBeFormatted[functionList[0]])
                try:
                    filestring = f'data/{tablekey}/{timestamp}.parquet'
                    write_file_to_processed_bucket(
                        processed_bucket, filestring, formatteddata)
                    logger.info(f'{tablekey} parquet updated')
                    if filestring not in updatedfiles:
                        updatedfiles.append(filestring)
                except Exception as e:
                    logger.error(e)

    df = pd.DataFrame(updatedfiles, columns=["File names"])
    out_buffer = BytesIO()
    df.to_csv(out_buffer, index=False)

    try:
        s3.put_object(Body=out_buffer.getvalue(),
                      Bucket=processed_bucket, Key="updatedfiles.csv")
    except ClientError as e:
        logger.error(e)
    except Exception:
        logger.error("Unknown error writing to Updated Files")
        raise Exception
