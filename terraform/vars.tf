variable "ingest_lambda_name" {
    description = "Ingest lambda function"
    type = string
    default = "ingest-data"
}

variable "aws_access_key" {
  type = string
}

variable "aws_secret_key" {
  type = string
}