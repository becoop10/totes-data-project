
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
    alarm_actions = ["arn:aws:sns:us-east-1:699459409711:test-error-alerts"]
    insufficient_data_actions = []
    treat_missing_data = "ignore"
}