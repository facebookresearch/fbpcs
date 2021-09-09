resource "aws_kms_key" "s3_kms_key" {
  description = "This key is used to encrypt bucket objects"
}

data "archive_file" "zip_lambda" {
  type        = "zip"
  source_file = "lambda_trigger.py"
  output_path = "semi-automated-data-ingestion/${var.lambda_trigger_s3_key}"
}

resource "aws_s3_bucket_object" "upload_lambda_trigger" {
  bucket = var.app_data_input_bucket_id
  key    = "semi-automated-data-ingestion/${var.lambda_trigger_s3_key}"
  source = "semi-automated-data-ingestion/${var.lambda_trigger_s3_key}"
  etag   = filemd5("lambda_trigger.py")

}

resource "aws_iam_role" "lambda_iam" {
  name = "lambda-trigger-iam${var.tag_postfix}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_lambda_function" "lambda_trigger" {
  s3_bucket     = var.app_data_input_bucket_id
  s3_key        = "semi-automated-data-ingestion/${var.lambda_trigger_s3_key}"
  function_name = "semi-automated-data-ingestion-trigger${var.tag_postfix}"
  role          = aws_iam_role.lambda_iam.arn
  handler       = "lambda_trigger.lambda_handler"
  runtime       = "python3.8"
  timeout       = 60
  environment {
    variables = {
      DEBUG = "false"
    }
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = var.app_data_input_bucket_id
  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".csv"
  }
}

resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_trigger.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.app_data_input_bucket_arn
}

resource "aws_iam_role_policy_attachment" "lambda_glue_service_role" {
  role       = aws_iam_role.lambda_iam.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch" {
  role       = aws_iam_role.lambda_iam.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "kms_policy_lambda" {
  name   = "lambda-kms-policy${var.tag_postfix}"
  role   = aws_iam_role.lambda_iam.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "kms:*",
      "Resource": [
        "arn:aws:kms:*:${var.aws_account_id}:key/*",
        "arn:aws:kms:*:${var.aws_account_id}:alias/*"
      ]
    }
  ]
}
EOF
}
