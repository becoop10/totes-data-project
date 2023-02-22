variable "TOTESYS_DB_HOST" {}
variable "TOTESYS_DB_PORT" {}
variable "TOTESYS_DB_DATABASE" {}
variable "TOTESYS_DB_USER" {}
variable "TOTESYS_DB_PASSWORD" {}

variable "DATA_WAREHOUSE_HOST" {}
variable "DATA_WAREHOUSE_PORT" {}
variable "DATA_WAREHOUSE_DATABASE" {}
variable "DATA_WAREHOUSE_USER" {}
variable "DATA_WAREHOUSE_PASSWORD" {}

resource "aws_secretsmanager_secret" "totesys-db-credentials" {
    name        = "totesys-db"
}

resource "aws_secretsmanager_secret_version" "totesys-db-credentials" {
    secret_id     = aws_secretsmanager_secret.totesys-db-credentials.id
    secret_string = jsonencode({
        host: "${var.TOTESYS_DB_HOST}"
        port: "${var.TOTESYS_DB_PORT}"
        dbname: "${var.TOTESYS_DB_DATABASE}"
        username: "${var.TOTESYS_DB_USER}"
        password: "${var.TOTESYS_DB_PASSWORD}"          
    })
}

resource "aws_secretsmanager_secret" "data-warehouse-credentials" {
    name        = "data-warehouse"
}

resource "aws_secretsmanager_secret_version" "data-warehouse-credentials" {
    secret_id     = aws_secretsmanager_secret.data-warehouse-credentials.id
    secret_string = jsonencode({
        host: "${var.DATA_WAREHOUSE_HOST}"
        port: "${var.DATA_WAREHOUSE_PORT}"
        dbname: "${var.DATA_WAREHOUSE_DATABASE}"
        username: "${var.DATA_WAREHOUSE_USER}"
        password: "${var.DATA_WAREHOUSE_PASSWORD}"          
    })
}
