resource "aws_lambda_function" "ingest_lambda" {
    filename = "../src/file_reader/query_totesys_db.zip"
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "query_totesys_db.lambda_handler"
    runtime = "python3.9"
}
