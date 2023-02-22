import logging
import boto3


def get_secret():

    secret_name = "data-warehouse"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

logger = logging.getLogger('dummyLambdaLogger')
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    db = get_secret()

    logger.info(db['host'])
    logger.info(db['port'])
    logger.info(db['dbname'])
    logger.info(db['username'])
    logger.info(db['password'])


    