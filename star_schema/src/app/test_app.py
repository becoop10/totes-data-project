from moto import mock_s3
import boto3
from star_schema.src.app.app import lambda_handler, get_bucket_name, get_file_names, get_file_contents,write_file_to_processed_bucket
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
    conn.upload_file('./star_schema/src/app/test.txt',
     'totes-amazeballs-s3-ingested-data-bucket-0987', 'test.txt')
    conn.upload_file('./star_schema/src/app/dummy.txt',
     'totes-amazeballs-s3-ingested-data-bucket-0987', 'dummy.txt')

    assert get_file_names('totes-amazeballs-s3-ingested-data-bucket-0987') == ['dummy.txt','test.txt']


@mock_s3
def test_get_file_contents():

    conn = boto3.client('s3')
    conn.create_bucket(Bucket='totes-amazeballs-s3-ingested-data-bucket-0987')
    conn.upload_file('./star_schema/src/app/address.json',
     'totes-amazeballs-s3-ingested-data-bucket-0987', 'address.json')
    

    
    assert get_file_names('totes-amazeballs-s3-ingested-data-bucket-0987') == ['address.json']
    with open("./star_schema/src/app/address.json") as f:   
        assert get_file_contents('totes-amazeballs-s3-ingested-data-bucket-0987','address.json')==json.loads(f.read())



@mock_s3
def test_writes_to_new_bucket():
    conn = boto3.client('s3')
    BUCKETNAME='totes-amazeballs-s3-processed-data-bucket-0987'
    conn.create_bucket(Bucket=BUCKETNAME)

    list=[{"hello":1,"goodbye":2},{"hello":3,"goodbye":4}]

    df=pd.DataFrame(list)

    write_file_to_processed_bucket(BUCKETNAME,'testoutput',df)
    response=conn.list_objects(Bucket=BUCKETNAME)
    
    response2=conn.get_object(Bucket=BUCKETNAME,Key='testoutput')
    
    
    
    file=pd.read_parquet(BytesIO(response2['Body'].read()))
    
    
    print(file)


    
    assert response['Contents'][0]['Key']=='testoutput'
    assert file.to_dict('records')==list



