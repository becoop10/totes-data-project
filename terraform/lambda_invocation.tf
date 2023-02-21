data "aws_lambda_invocation" "example" {
  function_name = aws_lambda_function.ingest_lambda.function_name

  input = <<JSON
{
  "key1": "value1",
  "key2": "value2"
}
JSON
}