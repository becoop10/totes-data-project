from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
from unittest.mock import patch
from src.transform_data import (lambda_handler, get_bucket_names, get_file_names,
 get_file_contents, write_file_to_processed_bucket)
import json
import os
import pytest
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
logger.propagate = True


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


@patch('src.transform_data.get_bucket_names', return_value=[])
def test_lambda_handler_logs_if_no_buckets_are_found(s3, caplog):
    with caplog.at_level(logging.INFO):
        lambda_handler({}, {})
        assert 'No buckets found.' in caplog.text

@patch('src.transform_data.get_bucket_names', return_value=['totes-amazeballs-s3-ingested', 'other-s3-bucket'])
def test_lambda_handler_logs_error_if_both_buckets_are_not_found(mock_buckets, s3, caplog):
    print(mock_buckets())
    with caplog.at_level(logging.INFO):
        lambda_handler({}, {})
        assert 'Error retreiving bucket names.' in caplog.text


# @patch('src.transform_data.boto3.client', return_value='s3')
# @patch('src.transform_data.get_timestamp', return_value='14:30')
# @patch('src.transform_data.get_file_names', return_value=[])
# def test_lambda_assigns_correct_value_to_ingested_bucket(mock_file_names, mock_get_timestamp, mock_s3):
#     with patch('src.transform_data.get_bucket_names', 
#                return_value=['totes-amazeballs-s3-ingested', 'processed-s3-bucket']) as mock_buckets:
#         lambda_handler({}, {})
#         mock_s3_obj = mock_s3()
#         mock_get_timestamp.assert_called_with(mock_s3_obj, 'totes-amazeballs-s3-ingested', 'data/timestamp.txt')


# @patch('src.transform_data.boto3.client', return_value='s3')
# @patch('src.transform_data.get_timestamp', return_value='14:30')
# @patch('src.transform_data.get_file_names')
# def test_lambda_calls_get_file_names_with_correct_timestamp(mock_get_file_names, mock_get_timestamp,  mock_s3):
#     with patch('src.transform_data.get_bucket_names', 
#                return_value=['totes-amazeballs-s3-ingested', 'processed-s3-bucket']):
#         lambda_handler({}, {})
#         mock_get_file_names.assert_called_with('totes-amazeballs-s3-ingested', 'data/14:30/')

# @patch('src.transform_data.boto3.client', return_value='s3')
# @patch('src.transform_data.get_timestamp', return_value='14:30')
# @patch('src.transform_data.get_file_names', return_value=['dummy1.txt', 'dummy2.txt', 'data/14:30/payment.json'])
# @patch('src.transform_data.get_file_contents')
# def test_lambda_reads_s3_when_(mock_get_contents, mock_get_file_names, mock_get_timestamp,  mock_s3):
#         with patch('src.transform_data.get_bucket_names', 
#                return_value=['totes-amazeballs-s3-ingested', 'processed-s3-bucket']):
#             lambda_handler({}, {})
#             mock_get_contents.assert_called_with('totes-amazeballs-s3-ingested', 'data/14:30/payment.json')

# @patch('src.transform_data.boto3.client', return_value='s3')
# @patch('src.transform_data.get_timestamp', return_value='14:30')
# @patch('src.transform_data.get_file_names', return_value=['dummy1.txt', 'dummy2.txt', 'data/14:30/payment.json'])
# @patch('src.transform_data.get_file_contents')
# def test_lambda_reads_s3_when_(mock_get_contents, mock_get_file_names, mock_get_timestamp,  mock_s3):
#         with patch('src.transform_data.get_bucket_names', 
#                return_value=['totes-amazeballs-s3-ingested', 'processed-s3-bucket']):
#             lambda_handler({}, {})
#             mock_get_contents.assert_called_with('totes-amazeballs-s3-ingested', 'data/14:30/payment.json')
