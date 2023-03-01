resource "aws_iam_role" "load_lambda_role" {
    name_prefix = "role-${var.load_lambda_name}"
    assume_role_policy = <<EOF
{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}


data "aws_iam_policy_document" "s3_load_document" {
    statement {
        actions = [
            "s3:PutObject",
            "s3:GetObject",
            ]

        resources = [
            "${aws_s3_bucket.processed_data.arn}/*",
        ]
    }
    statement {
        actions = ["lambda:InvokeFunction","lambda:InvokeFunctionConfiguration", "lambda:UpdateFunctionConfiguration"]

        resources = [
            "arn:aws:s3:::*"
        ]
    }
}

resource "aws_iam_policy" "s3_load_policy" {
    name_prefix = "s3-load-policy-${var.load_lambda_name}"
    policy = data.aws_iam_policy_document.s3_load_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_load_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.s3_load_policy.arn
}

data "aws_iam_policy_document" "cw_load_document" {
     statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.load_lambda_name}:*"
    ]
  }
}


resource "aws_iam_policy" "cw_load_policy" {
    name_prefix = "cw-policy-${var.load_lambda_name}"
    policy = data.aws_iam_policy_document.cw_load_document.json
}



resource "aws_iam_role_policy_attachment" "lambda_load_cw_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.cw_load_policy.arn
}


resource "aws_s3_bucket_notification" "load_bucket_notification" {
  bucket = aws_s3_bucket.processed_data.id
  depends_on = [
    aws_lambda_function.load_lambda,aws_iam_role_policy_attachment.lambda_s3_load_policy_attachment
  ]

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events              = ["s3:ObjectCreated:*"]
   
    filter_suffix       = ".csv"
  }

}

resource "aws_lambda_permission" "load_allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.load_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.processed_data.arn
  source_account = data.aws_caller_identity.current.account_id
}
