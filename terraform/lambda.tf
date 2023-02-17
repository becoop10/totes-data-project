resource "aws_lambda_function" "ingest_lambda" {
    filename = "../src/query_totesys_db.zip" # insert zip folder name here
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "query_totesy_db.lambda_handler"
    runtime = "python3.9"
}

data "aws_lambda_invocation" "example" {
  function_name = aws_lambda_function.ingest_lambda.function_name

  input = <<JSON
{
  "key1": "value1",
  "key2": "value2"
}
JSON
}