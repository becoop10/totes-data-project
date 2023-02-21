resource "aws_s3_bucket" "ingested_data" {
    bucket = "totes-amazeballs-s3-ingested-data-bucket"
}

resource "aws_s3_bucket" "processed_data" {
    bucket = "totes-amazeballs-s3-processed-data-bucket"
}

resource "aws_s3_bucket" "terraform_state_bucket" {
    bucket = "totes-amazeballs-s3-terraform-state-bucket"
}