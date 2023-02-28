data "archive_file" "ingest_lambda" {
  type        = "zip"
  source_dir = "../src/ingest_package"
  output_path = "../src/ingest_deployment.zip"
}

resource "aws_lambda_function" "ingest_lambda" {
    filename = "../src/ingest_deployment.zip"
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "ingest_data.lambda_handler"
    runtime = "python3.9"
    source_code_hash = data.archive_file.ingest_lambda.output_base64sha256
    timeout = "10"
}
