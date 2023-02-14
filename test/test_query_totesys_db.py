from src.query_totesys_db import (write_to_s3, read_db, format_data, lambda_handler)
import boto3
import botocore
from moto import mock_s3
from moto.core import patch_client
import aws
import pytest
import os
from unittest.mock import MagicMock, patch, DEFAULT
import json


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

def test_format_data_converts_to_json():
    data = {
        'Hello': 'World'
    }
    result = format_data(data)
    print(result)
    assert result == '{"Hello": "World"}'

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
#     print(dir(s3))
#     s3.add_client_error(
#         "head_object",
#         expected_params={"Bucket": bucket, "Key": "foobar"},
#         service_error_code="404",
#     )
#     s3.create_bucket(
#         Bucket=bucket,
#         CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
#     )
#     data = 'Hello World'

#     with pytest.raises(ValueError) as e:
#         write_to_s3(s3,'hello', data, bucket)

    
