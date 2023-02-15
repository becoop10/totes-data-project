import logging

def lambda_handler(event, context):
    logging.error("Error")
    print('Lambda function executed!')
    print("Event:", event)
    print("Context:", context)
    raise Exception("This is an error message")

