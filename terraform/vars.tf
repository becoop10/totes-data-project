variable "ingest_lambda_name" {
    description = "Ingest lambda function"
    type = string
    default = "ingest-data"
}

variable "transform_lambda_name" {
    description = "Transform lambda function"
    type = string
    default = "transform-data"
}

variable "load_lambda_name" {
    description = "Load lambda function"
    type = string
    default = "load-data"
}