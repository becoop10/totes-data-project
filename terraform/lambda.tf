resource "aws_lambda_function" "ingest_lambda" {
    filename = "../src/ingest_deployment.zip"
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "ingest_data.lambda_handler"
    runtime = "python3.9"
    timeout = "10"
}
