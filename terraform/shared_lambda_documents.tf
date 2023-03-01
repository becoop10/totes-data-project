
data "aws_iam_policy_document" "list_buckets_document"{
    statement {
        actions = [
          "s3:ListAllMyBuckets",
          "s3:ListBucket"
          ]

        resources = [
            "arn:aws:s3:::*"
        ]
    }

}

resource "aws_iam_policy" "list_buckets_policy" {
    name_prefix = "list-buckets-policy-"
    policy = data.aws_iam_policy_document.list_buckets_document.json
}

resource "aws_iam_role_policy_attachment" "list_buckets_ingest_policy_attachment" {
    role = aws_iam_role.ingest_lambda_role.name
    policy_arn = aws_iam_policy.list_buckets_policy.arn
}

resource "aws_iam_role_policy_attachment" "list_buckets_transform_policy_attachment" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.list_buckets_policy.arn
}

resource "aws_iam_role_policy_attachment" "list_buckets_load_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.list_buckets_policy.arn
}




resource "aws_iam_policy" "sm_policy" {
  name = "sm_access_permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_sm_policy_attachment" {
    role = aws_iam_role.ingest_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_sm_policy_attachment" {
    role = aws_iam_role.load_lambda_role.name
    policy_arn = aws_iam_policy.sm_policy.arn
}
