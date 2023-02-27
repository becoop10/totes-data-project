terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~>3.27"
    }
    klayers = {
      version = "~> 1.0.0"
      source  = "ldcorentin/klayer"
    }
  }

  required_version = ">=0.14.9"

  backend "s3" {
       bucket = "totes-amazeballs-s3-tfstate-2" # make this bucket before deployment
       key    = "terraform.tfstate"
       region = "us-east-1"
   }
}


provider "aws" {
  region  = "us-east-1"
}


resource "aws_s3_bucket" "terraform_state" {
  bucket = "totes-amazeballs-s3-tfstate-2" # make this bucket before deployment
}

resource "aws_s3_bucket_versioning" "terraform_state" {
    bucket = aws_s3_bucket.terraform_state.id

    versioning_configuration {
      status = "Enabled"
    }
}

resource "aws_dynamodb_table" "terraform_state_lock" {
  name           = "app-state"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}