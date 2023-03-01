resource "aws_lambda_function" "ingest_lambda" {
    filename = "../src/ingest_deployment1.zip"
    function_name = "${var.ingest_lambda_name}"
    role = aws_iam_role.ingest_lambda_role.arn
    handler = "ingest_data.lambda_handler"
    runtime = "python3.9"
    timeout = "10"
}

resource "aws_lambda_function" "transform_lambda" {
    filename = "../src/transform_deployment1.zip"
    function_name = "${var.transform_lambda_name}"
    role = aws_iam_role.transform_lambda_role.arn
    handler = "transform_data.lambda_handler"
    runtime = "python3.9"
    layers = ["arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-pandas:11",]
    timeout = "30"
}

resource "aws_lambda_function" "load_lambda" {
    filename = "../src/load_deployment.zip" # Put filepath to load zip here
    function_name = "${var.load_lambda_name}"
    role = aws_iam_role.load_lambda_role.arn
    handler = "load_data.lambda_handler" # Put lambda handler here
    runtime = "python3.9"
    layers = [
        "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-pandas:11"
        ]
    timeout = "900"
}