resource "aws_s3_bucket" "ingested_data" {
    bucket_prefix = "totes-amazeballs-s3-ingested"
}

resource "aws_s3_bucket" "processed_data" {
    bucket_prefix = "totes-amazeballs-s3-processed"
}