provider "aws" {
  profile = "default"
  region  = var.aws_region
}

provider "archive" {}

terraform {
  backend "s3" {}
}

data "archive_file" "lambda_source_package" {
  type        = "zip"
  source_dir  = "validation_utility/"
  excludes    = ["validation_utility/tests/*"]
  output_path = "validation_lambda${var.tag_postfix}.zip"
}

resource "aws_s3_bucket_object" "lambda_trigger_object" {
  bucket = var.upload_and_validation_s3_bucket
  key    = "data-validation-lambda-package${var.tag_postfix}.zip"
  source = data.archive_file.lambda_source_package.output_path
  etag   = filemd5("validation_lambda${var.tag_postfix}.zip")
}

resource "aws_iam_role" "lambda_iam" {
  name = "lambda-iam-data-validation${var.tag_postfix}"

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

resource "aws_lambda_function" "upload_lambda_trigger" {
  s3_bucket     = var.upload_and_validation_s3_bucket
  s3_key        = "data-validation-lambda-package${var.tag_postfix}.zip"
  function_name = "data-validation-lambda-trigger${var.tag_postfix}"
  role          = aws_iam_role.lambda_iam.arn
  handler       = "lambda_main.lambda_handler"
  runtime       = "python3.9"
  timeout       = 900
  memory_size   = 1024
  environment {
    variables = {
      VALIDATION_DEBUG_MODE           = var.validation_debug_mode
      UPLOAD_AND_VALIDATION_S3_BUCKET = var.upload_and_validation_s3_bucket
      EVENTS_DATA_UPLOAD_S3_KEY       = var.events_data_upload_s3_key
      VALIDATION_RESULTS_S3_KEY       = var.validation_results_s3_key
    }
  }
}

resource "aws_lambda_permission" "allow_upload_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.upload_lambda_trigger.arn
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.upload_and_validation_s3_bucket}"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = var.upload_and_validation_s3_bucket
  lambda_function {
    lambda_function_arn = aws_lambda_function.upload_lambda_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "${var.events_data_upload_s3_key}/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_upload_bucket]
}

resource "aws_iam_role_policy" "s3_policy_lambda_upload_bucket_key" {
  name   = "lambda-s3-policy${var.tag_postfix}"
  role   = aws_iam_role.lambda_iam.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": [
          "arn:aws:s3:::${var.upload_and_validation_s3_bucket}/${var.events_data_upload_s3_key}",
          "arn:aws:s3:::${var.upload_and_validation_s3_bucket}/${var.events_data_upload_s3_key}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "s3_policy_lambda_validation_bucket_key" {
  name   = "lambda-s3-policy${var.tag_postfix}"
  count  = var.validation_results_s3_key == "" ? 0 : 1
  role   = aws_iam_role.lambda_iam.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": [
          "arn:aws:s3:::${var.upload_and_validation_s3_bucket}/${var.validation_results_s3_key}",
          "arn:aws:s3:::${var.upload_and_validation_s3_bucket}/${var.validation_results_s3_key}/*"
      ]
    }
  ]
}
EOF
}
