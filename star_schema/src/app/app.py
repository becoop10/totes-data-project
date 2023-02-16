import boto3
from star_schema.src.utils.format_counterparty import format_counterparty
from star_schema.src.utils.format_currency import format_currency
from star_schema.src.utils.format_design import format_design
from star_schema.src.utils.format_location import format_location
from star_schema.src.utils.format_payment_type import format_payment_type
from star_schema.src.utils.format_payments import format_payments
from star_schema.src.utils.format_purchase_facts import format_purchase
from star_schema.src.utils.format_sales_facts import format_sales_facts
from star_schema.src.utils.format_staff import format_staff


def get_bucket_name():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]


bucket_names = get_bucket_name()
print(bucket_names)


def get_file_names(bucket_name):
    pass


def lambda_handler(event, context):
    pass


with open('/home/ben/northcoders/data-engineering/totes-data-project/star_schema/src/app/test.txt') as f:
    print(f)
