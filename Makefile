# Set variables
PYTHON = python3
PIP = pip
AWS_REGION = us-east-1
# change names as necessary
INGEST_LAMBDA_NAME = my-ingest-lambda
TRANSFORM_LAMBDA_NAME = my-transform-lambda
LOAD_LAMBDA_NAME = my-load-lambda

# Default target
all: deploy-ingest deploy-transform deploy-load

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Package the ingestion Lambda function code into a ZIP file
ingest-package:
	zip -r ingest.zip src/ingest_data.py

# Deploy the ingestion Lambda function to AWS, will need to be reconciled with github secrets for AWS tokens
deploy-ingest: ingest-package
	aws lambda create-function --function-name $(INGEST_LAMBDA_NAME) --runtime python3.8 --handler src.ingest_data.lambda_handler --role arn:aws:iam::123456789012:role/lambda-execution-role --zip-file fileb://ingest.zip --region $(AWS_REGION)

# Package the transformation Lambda function code into a ZIP file
transform-package:
	zip -r transform.zip src/transform_data.py

# Deploy the transformation Lambda function to AWS, will need to be reconciled with github secrets for AWS tokens
deploy-transform: transform-package
	aws lambda create-function --function-name $(TRANSFORM_LAMBDA_NAME) --runtime python3.8 --handler src.transform_data.lambda_handler --role arn:aws:iam::123456789012:role/lambda-execution-role --zip-file fileb://transform.zip --region $(AWS_REGION)

# Package the load Lambda function code into a ZIP file
load-package:
	zip -r load.zip src/load_data.py

# Deploy the load Lambda function to AWS, will need to be reconciled with github secrets for AWS tokens
deploy-load: load-package
	aws lambda create-function --function-name $(LOAD_LAMBDA_NAME) --runtime python3.8 --handler src.load_data.lambda_handler --role arn:aws:iam::123456789012:role/lambda-execution-role --zip-file fileb://load.zip --region $(AWS_REGION)

# Clean up build artifacts
clean:
	rm -f ingest.zip
	rm -f transform.zip
	rm -f load.zip
