resource "aws_s3_bucket" "ingested_data" {
  bucket_prefix = "s3-ingested-data-bucket-"
}

resource "aws_s3_bucket" "processed_data" {
  bucket_prefix = "s3-processed-data-bucket-"
}
