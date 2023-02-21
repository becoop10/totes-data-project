terraform {
  backend "s3" {
    bucket = "totes-amazeballs-s3-terraform-state-bucket-12345"
    key = "terraform.tfstate"
    region = "us-east-1"

    # both access and secret keys will need to be changed for github secrets once that is working
    # access_key = "${var.aws_access_key}"
    # secret_key = "${var.aws_secret_key}"
  }
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = "us-east-1"
}