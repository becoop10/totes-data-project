resource "aws_cloudwatch_event_rule" "scheduler" {
    name_prefix = "data-ingester-scheduler-"
    schedule_expression = "rate(10 minutes)"

}

resource "aws_lambda_permission" "allow_scheduler" {
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.ingest_lambda.function_name  #THIS NEEDS TO BE CHANGED FOR THE REAL ONE
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.scheduler.arn
    source_account = data.aws_caller_identity.current.account_id
  
}

resource "aws_cloudwatch_event_target" "lambda_target" {
    rule = aws_cloudwatch_event_rule.scheduler.name
    arn  = aws_lambda_function.ingest_lambda.arn #THIS NEEDS TO BE CHANGED FOR THE REAL ONE
}