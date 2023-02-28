resource "aws_sns_topic" "test_error_alerts" {
  name = "test-error-alerts"
  }

  resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.test_error_alerts.arn
  protocol = "email"
  endpoint = local.emails # email in stored in local.tf
}

resource "aws_cloudwatch_log_group" "ingest_lambda_log_group" {
  name = "/aws/lambda/${var.ingest_lambda_name}"
  }

resource "aws_cloudwatch_log_metric_filter" "any_error" {
    name = "any_error_notification"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/${var.ingest_lambda_name}"
    depends_on = [
      aws_cloudwatch_log_group.ingest_lambda_log_group
    ]
    metric_transformation {
      name = "error_metric"
      namespace = "ErrorMetrics"
      value = "1"
    }
}

resource "aws_cloudwatch_metric_alarm" "alert_errors" {
    alarm_name = "error_alert"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = "1"
    metric_name = "error_metric"
    namespace = "ErrorMetrics"
    period = "60"
    statistic = "Sum"
    threshold = "1"
    alarm_description = "Metric filter activates alarm when error found"
    actions_enabled = "true"
    alarm_actions = [aws_sns_topic.test_error_alerts.arn]
    insufficient_data_actions = []
    treat_missing_data = "ignore"
}

resource "aws_cloudwatch_log_group" "transform_lambda_log_group" {
  name = "/aws/lambda/${var.transform_lambda_name}"
  }

resource "aws_cloudwatch_log_metric_filter" "any_transform_error" {
    name = "any_error_notification"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/${var.transform_lambda_name}"
    depends_on = [
      aws_cloudwatch_log_group.transform_lambda_log_group
    ]
    metric_transformation {
      name = "error_metric"
      namespace = "ErrorMetrics"
      value = "1"
    }
}

resource "aws_cloudwatch_log_group" "load_lambda_log_group" {
  name = "/aws/lambda/${var.load_lambda_name}"
  }

resource "aws_cloudwatch_log_metric_filter" "any_load_error" {
    name = "any_error_notification"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/${var.load_lambda_name}"
    depends_on = [
      aws_cloudwatch_log_group.load_lambda_log_group
    ]
    metric_transformation {
      name = "error_metric"
      namespace = "ErrorMetrics"
      value = "1"
    }
}

resource "aws_cloudwatch_log_metric_filter" "duration_error" {
  name           = "LongErrorCounter"
  pattern        = "Duration"
  log_group_name = "/aws/lambda/${var.load_lambda_name}"

  metric_transformation {
    name      = "EventCount"
    namespace = "LongLambdaErrorCounter"
    value     = "1"
  }

}

resource "aws_cloudwatch_metric_alarm" "long_alert" {
  alarm_name          = "Long-Error"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "60"
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  threshold           = "6000"
  statistic           = "Maximum"
  alarm_actions       = [aws_sns_topic.test_error_alerts.arn]

}