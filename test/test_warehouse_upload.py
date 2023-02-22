from src.warehouse_upload import (
    read_csv, read_parquet, write_to_db, query_builder, data_sorter)
import pytest
import pandas as pd
from io import BytesIO
from moto import mock_s3
import boto3


@mock_s3
def test_read_parquet_reads_from_parquet_at_path():
    conn = boto3.client('s3')
    BUCKETNAME = 'totes-amazeballs-s3-processed-data-bucket-0987'
    conn.create_bucket(Bucket=BUCKETNAME)
    list = [{"hello": 1, "goodbye": 2}, {"hello": 3, "goodbye": 4}]
    df = pd.DataFrame(list)
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    conn.put_object(Bucket=BUCKETNAME, Key='test_parquet',
                    Body=out_buffer.getvalue())
    result = read_parquet(conn, 'test_parquet', BUCKETNAME)

    assert result == list


@mock_s3
def test_read_csv_reads_from_csv_at_path():
    conn = boto3.client('s3')
    BUCKETNAME = 'totes-amazeballs-s3-processed-data-bucket-0987'
    conn.create_bucket(Bucket=BUCKETNAME)
    list = ["hello", "goodbye"]
    df = pd.DataFrame(list, columns=['File names'])
    out_buffer = BytesIO()
    df.to_csv(out_buffer, index=False)
    conn.put_object(Bucket=BUCKETNAME, Key='test_csv',
                    Body=out_buffer.getvalue())
    result = read_csv(conn, 'test_csv', BUCKETNAME)
    assert result == list


@mock_s3
def test_query_builder_writes_query():
    r = {'staff_id': 1, 'hello': 2, 'world': 3}
    query, var_in = query_builder(r, 'dim_staff')
    expected = "INSERT INTO dim_staff (%s) VALUES (%s) ON CONFLICT (staff_id) DO UPDATE SET staff_id = EXCLUDED.staff_id, hello = EXCLUDED.hello, world = EXCLUDED.world;"
    assert query == expected


def test_data_sorter_sorts_list_of_dicts():
    d = [
        {'staff_id': 2},
        {'staff_id': 3},
        {'staff_id': 1}
    ]
    e = [
        {'staff_id': 1},
        {'staff_id': 2},
        {'staff_id': 3}
    ]
    result = data_sorter(d, 'dim_staff')
    assert result == e


@mock_s3
