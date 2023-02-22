from src.query_totesys_db import (write_to_s3, read_from_s3, read_db,
                                  format_data, retrieve_timestamp, write_timestamp, lambda_handler)
import boto3
import botocore
from moto import mock_s3
from moto.core import patch_client
import pytest
import os
from unittest.mock import MagicMock, patch, DEFAULT
import json
import datetime
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
    headers = read_db(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'currency';", ())
    result = format_data(data, headers)
    expected = [{
        'currency_id': 1,
        'currency_code': 'GBP',
        'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
    }]
    assert result == json.dumps(
        expected, indent=4, sort_keys=True, default=str)


def test_write_to_s3_writes_to_bucket(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    data = 'Hello World'
    write_to_s3(s3, 'hello', data, bucket)
    contents = s3.list_objects(Bucket=bucket)['Contents']
    assert contents[0]['Key'] == 'data/hello.json'


def test_write_to_s3_writes_correct_data_to_file(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    data = 'Hello World'
    write_to_s3(s3, 'hello', data, bucket)
    object_content = s3.get_object(Bucket=bucket, Key='data/hello.json')
    object_content = object_content['Body'].read().decode('ascii')
    assert object_content == data


def test_write_to_s3_overwrites_existing_file(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    data = 'Hello World'
    write_to_s3(s3, 'hello', data, bucket)
    data = 'Hello World Again'
    write_to_s3(s3, 'hello', data, bucket)
    object_content = s3.get_object(Bucket=bucket, Key='data/hello.json')
    object_content = object_content['Body'].read().decode('ascii')
    assert object_content == data


def test_read_from_s3_reads_data(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    data = 'Hello World'
    write_to_s3(s3, 'hello', data, bucket)
    content = read_from_s3(s3, bucket, 'data/hello.json')
    assert content == data


def test_read_from_s3_handles_no_such_key(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    content = read_from_s3(s3, bucket, 'helloWorld')
    assert content == {}


@freeze_time('2023-01-01')
def test_write_timestamp_writes_to_env_variable():
    write_timestamp()
    assert os.environ['totesys_last_read'] == datetime.datetime(
        2023, 1, 1).isoformat()


@freeze_time('2023-01-01')
def test_retrieve_timestamp_returns_stored_env_variable():
    write_timestamp()
    assert retrieve_timestamp() == datetime.datetime(2023, 1, 1).isoformat()


def test_retrieve_timestamp_returns_standard_when_no_date_stored():
    os.environ.pop('totesys_last_read')
    assert retrieve_timestamp() == datetime.datetime(1970, 1, 1).isoformat()


def test_lambda_handler_uploads_objects_to_s3(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    with patch('boto3.client', return_value=s3):
        lambda_handler({}, {})
        objects = s3.list_objects(Bucket=bucket)
        keys = [c['Key'] for c in objects['Contents']]
        tables = [
            'counterparty',
            'currency',
            'department',
            'design',
            'staff',
            'sales_order',
            'address',
            'payment',
            'purchase_order',
            'payment_type',
            'transaction'
        ]
        for t in tables:
            assert f'data/{t}.json' in keys


@freeze_time('2023-02-01')
def test_lambda_handler_updates_timestamp(s3):
    bucket = 'totes-amazeballs-s3-ingested-data-bucket-12345'
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': 'eu-east-2'}
    )
    with patch('boto3.client', return_value=s3):
        lambda_handler({}, {})
        ts = retrieve_timestamp()
        assert ts == datetime.datetime(2023, 2, 1).isoformat()
