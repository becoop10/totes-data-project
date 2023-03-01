resource "aws_iam_role" "transform_lambda_role" {
    name_prefix = "role-${var.transform_lambda_name}"
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

data "aws_iam_policy_document" "s3_transformed_document" {
    statement {
        actions = ["s3:PutObject"]

        resources = [
            "${aws_s3_bucket.processed_data.arn}/*"
        ]
    }
}

resource "aws_iam_policy" "s3_transformed_policy" {
    name_prefix = "s3-transformed-policy-${var.transform_lambda_name}"
    policy = data.aws_iam_policy_document.s3_transformed_document.json
}

resource "aws_iam_role_policy_attachment" "s3_transformed_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_transformed_policy.arn
}

data "aws_iam_policy_document" "s3_transformed_fetch_document" {
    statement {
        actions = ["s3:GetObject"]

        resources = [
            "${aws_s3_bucket.ingested_data.arn}/*",
        ]
    }
}

resource "aws_iam_policy" "s3_transformed_fetch_policy" {
    name_prefix = "s3-transformed-fetch-policy-${var.ingest_lambda_name}"
    policy = data.aws_iam_policy_document.s3_transformed_fetch_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_transformed_fetch_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_transformed_fetch_policy.arn
}

data "aws_iam_policy_document" "cw_transformed_document" {
     statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transform_lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "cw_transformed_policy" {
    name_prefix = "cw-policy-${var.transform_lambda_name}"
    policy = data.aws_iam_policy_document.cw_transformed_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_transformed_cw_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.cw_transformed_policy.arn
}


resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingested_data.id
  depends_on = [
    aws_lambda_function.transform_lambda,aws_iam_role_policy_attachment.lambda_s3_transformed_fetch_policy_attachment
  ]

  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
   
    filter_suffix       = ".txt"
  }

}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.transform_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingested_data.arn
  source_account = data.aws_caller_identity.current.account_id
}

