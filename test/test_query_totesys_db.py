from src.file_reader.query_totesys_db import (write_to_s3, read_db, format_data,retrieve_timestamp, write_timestamp, lambda_handler)
import boto3
import botocore
from moto import mock_s3
from moto.core import patch_client
import aws
import pytest
import os
from unittest.mock import MagicMock, patch, DEFAULT
import json
import datetime
import time
from freezegun import freeze_time

@pytest.fixture(scope='function')
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-east-2'

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-east-2')

@pytest.fixture
def event(self):
    return {

    }
@pytest.fixture
def context(self):
    return None
        
def test_read_db_returns_data_from_totesys_with_no_var_in():
    query = 'SELECT currency_code FROM currency WHERE currency_id = 1;'
    data = read_db(query, ())
    assert data[0][0] == 'GBP'

def test_read_db_returns_data_from_totesys_with_var_in():
    query = 'SELECT currency_code FROM currency WHERE currency_code = %s ;'
    data = read_db(query, ('GBP',))
    assert data[0][0] == 'GBP'


def test_format_data_converts_tuple_with_headings():
    data = read_db('SELECT * FROM currency LIMIT 1;', ())
    headers = read_db("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'currency';", ())
    result = format_data(data, headers)
    expected = [{
        'currency_id' : 1,
        'currency_code': 'GBP',
        'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
    }]
    assert result == json.dumps( expected, indent=4, sort_keys=True, default=str)

def test_write_to_s3_writes_to_bucket(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
        )
    data = 'Hello World'
    write_to_s3(s3,'hello', data, bucket)
    contents = s3.list_objects(Bucket=bucket)['Contents']
    assert contents[0]['Key'] == 'data/hello.json'

# def test_write_to_s3_handles_connection_error(s3):
#     # print(dir(botocore.client.S3EndpointSetter))

#     bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
#     s3.create_bucket(
#         Bucket=bucket,
#         CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
#     )
#     with patch(f's3.Bucket({bucket}).put_obect', side_effect=ValueError):   
#         data = 'Hello World'
#         with pytest.raises(Exception) as e:
#             write_to_s3(s3,'hello', data, bucket)

@freeze_time('2023-01-01')
def test_write_timestamp_writes_to_env_variable():
    write_timestamp()
    assert os.environ['totesys_last_read'] == datetime.datetime(2023,1,1).isoformat()

@freeze_time('2023-01-01')
def test_retrieve_timestamp_returns_stored_env_variable():
    write_timestamp()
    assert retrieve_timestamp() == datetime.datetime(2023,1,1).isoformat()
