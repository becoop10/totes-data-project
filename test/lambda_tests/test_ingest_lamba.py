from src.ingest_data import db_connect, lambda_handler, get_secret, read_db, get_timestamp, get_bucket_names, format_data, write_to_s3
from unittest.mock import MagicMock, patch, DEFAULT, Mock
from psycopg2.errors import OperationalError
import boto3
import botocore
from moto import mock_s3, mock_secretsmanager
from moto.core import patch_client
import pytest
import os
import json
import datetime
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


@pytest.fixture
def json_data():
    with open("test/lambda_tests/test_data/address.json") as f:
        return f.read()


@pytest.fixture
def event(self):
    return {}


@pytest.fixture
def context(self):
    return None


@patch("psycopg2.connect")
def test_read_db_returns_data_from_totesys_database(mock_connect):
    expected = [('GBP',)]
    mock_connect.cursor.return_value.fetchall.return_value = expected
    query = 'SELECT currency_code FROM currency WHERE currency_id = 1;'
    assert read_db(query, (), mock_connect) == expected


def test_get_timestamp_returns_timestamp_as_string(s3):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    with open("test/lambda_tests/test_data/timestamp.txt") as f:
        s3.upload_file(Filename="test/lambda_tests/test_data/timestamp.txt",
                       Bucket=bucket_name, Key='timestamp.txt')
        timestamp_returned = get_timestamp(s3, bucket_name, 'timestamp.txt')
        assert timestamp_returned == f.read()
        assert isinstance(timestamp_returned, str) == True


def test_get_timestamp_raises_exception_if_unable_to_retrieve(s3):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    with pytest.raises(Exception):
        get_timestamp(s3, bucket_name, 'timestamp.txt')


def test_get_timestamp_raises_exception_if_timestamp_is_in_incorrect_format(s3, caplog):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    with open("test/lambda_tests/test_data/dodgy_timestamp.txt") as f:
        s3.upload_file(Filename="test/lambda_tests/test_data/dodgy_timestamp.txt",
                       Bucket=bucket_name, Key='timestamp.txt')
        with pytest.raises(Exception):
            get_timestamp(s3, bucket_name, 'timestamp.txt')
        with caplog.at_level(logging.INFO):
            assert ('Timestamp file contents unacceptable.'
                    in caplog.text)

def test_get_bucket_name_assigns_correct_bucket_names_to_variables(s3):

    s3.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    s3.create_bucket(Bucket='totes-amazeballs-s3-processed-data-bucket-0987')

    assert get_bucket_names(s3) == ['totes-amazeballs-s3-ingested-data-bucket-0987',
                                  'totes-amazeballs-s3-processed-data-bucket-0987']


def test_format_data_converts_tuple_with_headings():
    test_currency_data = [(1, 'GBP', datetime.datetime(
        2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000))]
    test_currency_headers = [
        ('currency_id',), ('currency_code',), ('created_at',), ('last_updated',)]
    result = format_data(test_currency_data, test_currency_headers)
    expected = [{
        'currency_id': 1,
        'currency_code': 'GBP',
        'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
    }]
    assert result == json.dumps(
        expected, indent=4, sort_keys=True, default=str)


def test_write_to_s3_writes_to_bucket(s3, json_data):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    write_to_s3(s3, 'address', json_data, bucket_name)
    contents = s3.list_objects(Bucket=bucket_name)['Contents']
    assert contents[0]['Key'] == 'data/address.json'


def test_write_to_s3_writes_correct_data_to_file(s3, json_data):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    write_to_s3(s3, 'address', json_data, bucket_name)
    object_content = s3.get_object(Bucket=bucket_name, Key='data/address.json')
    object_content = object_content['Body'].read().decode('ascii')
    assert object_content == json_data


def test_write_to_s3_logs_error_if_unable_to_write(s3, json_data, caplog):
    with caplog.at_level(logging.INFO):
        write_to_s3(s3, 'address', json_data,
                    'totes-amazeballs-bucket-which-doesnt-exist')
        assert ('Bucket totes-amazeballs-bucket-which-doesnt-exist not found'
                in caplog.text)


def test_write_to_s3_overwrites_existing_file(s3, json_data):
    bucket_name = 'totes-amazeballs-s3-ingested-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    write_to_s3(s3, 'address', json_data, bucket_name)
    with open("test/lambda_tests/test_data/new_address.json") as f:
        new_data = f.read()
        write_to_s3(s3, 'address', new_data, bucket_name)
        object_content = s3.get_object(
            Bucket=bucket_name, Key='data/address.json')
        object_content = object_content['Body'].read().decode('ascii')
        assert object_content == new_data


@mock_secretsmanager
def test_get_secret_returns_dict_secret_if_exists():
    conn = boto3.client("secretsmanager", region_name="us-west-1")
    with open("test/lambda_tests/test_data/secret.json") as s:
        secret_to_add = s.read()
        conn.create_secret(
            Name='totesys-db',
            SecretString=secret_to_add,
        )
        secret_retrieved = get_secret(conn)
        assert secret_retrieved == json.loads(secret_to_add)
        assert secret_retrieved['host'] == "127.0.0.1"
        assert secret_retrieved['username'] == "project_team_5"
        assert secret_retrieved['port'] == 5432
        assert secret_retrieved['dbname'] == "dumm_db"
        assert secret_retrieved['password'] == "hello world"


@mock_s3
@patch('src.ingest_data.get_secret', return_value={
    "username": "project_team_5",
    "password": "hello world",
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "dumm_db"
})
@patch("psycopg2.connect")
def test_db_connect_creates_db_connection_with_secret_credentials(mock_connect, mock_get_secret):
    mock_client = boto3.client('secretsmanager', 'us-east-1')
    db_connect(mock_client)
    mock_connect.assert_called_with(host='127.0.0.1',
                                    database='dumm_db',
                                    user='project_team_5',
                                    password='hello world',
                                    port=5432)


@patch('src.ingest_data.db_connect', side_effect=OperationalError)
def test_lambda_handler_raises_error_connecting_to_db(mock_connect, s3, caplog):
    with pytest.raises(OperationalError):
        lambda_handler({}, {})


@patch('src.ingest_data.db_connect', return_value='test connection')
@patch('src.ingest_data.get_bucket_names', return_value=[])
def test_lambda_handler_logs_if_no_buckets_are_found(mock_get_buckets, mock_connect, s3, caplog):
    with caplog.at_level(logging.INFO):
        lambda_handler({}, {})
        assert 'No buckets found.' in caplog.text

@patch('src.ingest_data.db_connect', return_value='test connection')
@patch('src.ingest_data.get_bucket_names', return_value=['processed-bucket'])
def test_lambda_handler_logs_if_ingested_bucket_doesnt_exist(mock_get_buckets, mock_connect, s3, caplog):
    with pytest.raises(KeyError):
        lambda_handler({}, {})

@patch('src.ingest_data.db_connect')
@patch('src.ingest_data.get_bucket_names', return_value=['processed-bucket', 'ingested_bucket'])
@patch('src.ingest_data.read_db', return_value=[])
def test_lambda_handler_queries_db_for_every_table(mock_read_db, mock_get_buckets, mock_connect, s3):
    with patch('src.ingest_data.get_timestamp', return_value='2023-02-20 09:00:35.185169') as mock_timestamp:
        mock_conn = mock_connect.return_value
        mock_conn.close.return_value = ''
        lambda_handler({}, {})
        assert mock_read_db.call_count == 11