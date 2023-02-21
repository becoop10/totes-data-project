resource "aws_s3_bucket" "ingested_data" {
    bucket = "totes-amazeballs-s3-ingested-data-bucket-alasdair-122345"
}

resource "aws_s3_bucket" "processed_data" {
    bucket = "totes-amazeballs-s3-processed-data-bucket-alasdair-12345"
}

