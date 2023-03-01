from moto import mock_s3
import boto3
from utils.myutils import (get_bucket_names, get_file_names,
 get_file_contents, write_file_to_processed_bucket)
import json
import os
import pytest
import pandas as pd
from io import BytesIO

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials, to ensure we're not touching AWS directly"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")

def test_get_bucket_name_assigns_correct_bucket_names_to_variables(s3):
    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    s3.create_bucket(Bucket='totes-amazeballs-s3-processed-data-bucket-0987')

    assert get_bucket_names(s3) == ['totes-amazeballs-s3-ingested-data-bucket-0987',
                                  'totes-amazeballs-s3-processed-data-bucket-0987']
    
def test_get_bucket_names_returns_empty_list_when_there_are_no_buckets(s3):

    assert get_bucket_names(s3) == []

def test_get_file_names_correctly_retrieves_file_names_in_ingested_bucket(s3):

    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-0987')
    s3.upload_file('./star_schema/src/app/test.txt',
                   'totes-amazeballs-s3-ingested-0987', '2022-11-03 14:20:49.962000/test.txt')
    s3.upload_file('./star_schema/src/app/dummy.txt',
                   'totes-amazeballs-s3-ingested-0987', '2022-11-03 14:20:49.962000/dummy.txt')
    assert get_file_names(
        'totes-amazeballs-s3-ingested-0987', '2022-11-03 14:20:49.962000') == ['2022-11-03 14:20:49.962000/dummy.txt', '2022-11-03 14:20:49.962000/test.txt']


def test_get_file_names_returns_empty_list_when_no_files_are_in_directory(s3):

    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-0987')
    assert get_file_names('totes-amazeballs-s3-ingested-0987',
                          '2022-11-03 14:20:49.962000') == []


def test_get_file_names_ignores_other_dates(s3):

    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-0987')
    s3.upload_file('./star_schema/src/app/test.txt',
                   'totes-amazeballs-s3-ingested-0987', '2022-11-03 15:20:51.962000/test.txt')
    s3.upload_file('./star_schema/src/app/dummy.txt',
                   'totes-amazeballs-s3-ingested-0987', '2022-11-03 15:20:51.962000/dummy.txt')
    s3.upload_file('./star_schema/src/app/dummy.txt',
                   'totes-amazeballs-s3-ingested-0987', '2022-11-03 14:20:49.962000/bingo.txt')
    assert get_file_names(
        'totes-amazeballs-s3-ingested-0987', '2022-11-03 14:20:49.962000') == ['2022-11-03 14:20:49.962000/bingo.txt']


def test_get_file_contents_returns_correct_file_content_as_json(s3):

    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    s3.upload_file('./star_schema/src/app/address.json',
                   'totes-amazeballs-s3-ingested-data-bucket-0987', 'address.json')

    assert get_file_names(
        'totes-amazeballs-s3-ingested-data-bucket-0987', '') == ['address.json']
    with open("./star_schema/src/app/address.json") as f:
        assert get_file_contents(
            'totes-amazeballs-s3-ingested-data-bucket-0987', 'address.json') == json.loads(f.read())


def test_write_file_writes_data_to_processed_bucket(s3):

    test_bucket_name = 'totes-amazeballs-s3-processed-data-bucket-0987'
    s3.create_bucket(Bucket=test_bucket_name)

    list = [{"hello": 1, "goodbye": 2}, {"hello": 3, "goodbye": 4}]

    df = pd.DataFrame(list)

    write_file_to_processed_bucket(test_bucket_name, 'testoutput', df)
    response = s3.list_objects(Bucket=test_bucket_name)

    response2 = s3.get_object(Bucket=test_bucket_name, Key='testoutput')

    file = pd.read_parquet(BytesIO(response2['Body'].read()))

    assert response['Contents'][0]['Key'] == 'testoutput'
    assert file.to_dict('records') == list

