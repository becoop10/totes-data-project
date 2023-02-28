from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
from src.transform_data import (lambda_handler, get_bucket_names, get_file_names,
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


@patch('src.transform_data.get_bucket_names' )
def lambda_handler_logs_if_error_retrieving_bucket_names(s3):
    
