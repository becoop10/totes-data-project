resource "aws_lambda_function" "ingest_lambda" {
    filename = "../function.zip" # insert zip folder name here
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "query_totesy_db.lambda_handler"
    runtime = "python3.9"
}

