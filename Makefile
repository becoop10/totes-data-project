# Set variables
PYTHON = python3
PIP = pip
AWS_REGION = us-east-1

# Default target
all: terraform-init terraform-plan terraform-apply

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Package the ingestion Lambda function code into a ZIP file
ingest-package:
	zip -r ingest.zip src/ingest_data.py

# Package the transformation Lambda function code into a ZIP file
transform-package:
	zip -r transform.zip src/transform_data.py

# Package the load Lambda function code into a ZIP file
load-package:
	zip -r load.zip src/load_data.py

# Clean up build artifacts
clean:
	rm -f ingest.zip
	rm -f transform.zip
	rm -f load.zip

# Set variables
TERRAFORM_DIR = terraform

# Initialize Terraform
terraform-init:
	cd $(TERRAFORM_DIR) && terraform init

# Generate and show Terraform execution plan
terraform-plan:
	cd $(TERRAFORM_DIR) && terraform plan

# Apply Terraform changes
terraform-apply:
	cd $(TERRAFORM_DIR) && terraform apply
