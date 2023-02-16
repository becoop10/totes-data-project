from moto import mock_s3
import boto3
from star_schema.src.app.app import lambda_handler, get_bucket_name, get_file_names
import pytest
import os


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials, to ensure we're not touching AWS directly"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@mock_s3
def test_get_bucket_name_assigns_correct_bucket_names_to_variables():

    conn = boto3.client('s3')
    conn.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    conn.create_bucket(Bucket='totes-amazeballs-s3-processed-data-bucket-0987')

    assert get_bucket_name() == ['totes-amazeballs-s3-ingested-data-bucket-0987',
                                 'totes-amazeballs-s3-processed-data-bucket-0987']


@mock_s3
def test_get_file_names_correctly_retrieves_file_names_in_ingested_bucket():

    conn = boto3.client('s3')
    conn.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    # conn.upload_file('test.txt',
    #  'totes-amazeballs-s3-ingested-data-bucket-0987', 'test.txt')

    # assert get_file_names() == ['test.txt']


with open('./test.txt') as f:
    print(f)
