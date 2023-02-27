# totes-data-project

## Overview

This repo was created during the project phase of the Northcoders Data Engineering Bootcamp in February 2023. We were supplied with a source database and were given the task of extracting, transforming and loading the data to a data warehouse forming a basic ETL data pipeline. This was all to be deployed on Amazon Web Services with S3 buckets and a series of Lambda functions.

This project was undertaken by a group of 6 over 2 weeks and developed using Agile working.

## Key Objectives

- Transform data from third normal form so it can be loaded to fact and dimension tables in the data warehouse.
- Data appearing in the source database should appear in the data warehouse within 30 minutes.

## Pipeline Diagram

![Screenshot](/diagram.png)

## Dependencies and technologies used

- Python, Psycopg 2 and Pandas to extract, transform and load data to the data warehouse. Use of Boto 3 to interact with AWS infrastructure.
- Entire pipeline is deployed on to AWS using Terraform to create the infrastructure.
- Use of a workflow with GitHub Actions to trigger deployment.

## Source Database

Data is supplied by Northcoders in to tables in a Postgresql OLTP database which is meant to simulate the back end data of a commercial application. Entries were added and updated at random intervals.

The ERD for this database can be seen here:
(https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92)

## Extract

Data extracted from the source database was written to the S3 landing bucket first in JSON format. The 'extract' Lambda function is triggered by an EventBridge Scheduler every ten minutes. To only query the source database for new or updated data, we used a timestamp file that updates when new or updated data is found. The lambda logs to CloudWatch Logs when tables have been updated. When serious errors occur, an email notification will be sent.

If there is no updated tables in the source database, the cycle will stop here.

## Transform

On the update of the timestamp file in the landing bucket, the second lambda is triggered using S3 Event Notifications. The transforming lambda uses a series of python functions to format the data so that it can be loaded to the fact and dimension tables in the data warehouse. This tranformed data is then written to the S3 processed bucket in parquet format. A CSV file containing the table(s) that have new or updated data is then written to the processed bucket.

## Load

On the update of the CSV file in the processed bucket, the third lambda is triggered. This lambda reads the CSV files for updated files, reads the relevant parquet files and loads this data to the tables in the data warehouse. Duplicate data will not be added, and updated data will overwrite the old entry.

## Data Warehouse

Data here is stored in fact and dimension tables. There are three Fact tables (sales, purchase orders and payments) sharing dimension tables forming a Galaxy Schema.

The ERD for this database can be seen here:
(https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca)

## Setup

In the root of the repository, you will find a Makefile, and a run_tests.sh file. These files need to be run for setup.

The Makefile is a set of commands to automate the packaging and deployment of AWS Lambda functions and Terraform infrastructure. The install target installs dependencies listed in the requirements.txt file. The ingest-package, transform-package, and load-package targets package the Lambda functions' code into ZIP files. The clean target removes the build artifacts. The terraform-init, terraform-plan, and terraform-apply targets initialize Terraform, show the execution plan, and apply the changes, respectively. Before using this script, you will need to run the makefile script first by executing the command make from the root of the directory.

The run_tests.sh bashscript file is a set of commands that automate code quality checks and asks for user confirmation before applying them. The first command runs pytest on the specified directories. The next few commands check for autopep8 compliance on several directories and prompt the user to apply the changes if any were found. To use this script, you will need to run chmod +x run_tests.sh in your terminal to allow permissions to run the file, and then execute the script with the following command from the root of the directory: ./run_tests.sh.
