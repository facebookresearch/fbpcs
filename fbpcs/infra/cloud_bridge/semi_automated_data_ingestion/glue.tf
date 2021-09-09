resource "aws_s3_bucket_object" "upload_glue_ETL" {
  bucket = "${var.app_data_input_bucket_id}"
  key    = "semi-automated-app-data-ingestion/glue_ETL.py"
  source = "glue_ETL.py"
  etag   = filemd5("glue_ETL.py")
}

resource "aws_iam_role" "glue_ETL_role" {
  name               = "glue-ETL-role${var.tag_postfix}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_ETL_role.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "attach_s3_access" {
  role       = aws_iam_role.glue_ETL_role.id
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy" "s3_policy" {
  name   = "s3-policy${var.tag_postfix}"
  role   = aws_iam_role.glue_ETL_role.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::${var.app_data_input_bucket}",
        "arn:aws:s3:::${var.app_data_input_bucket}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "kms_policy_glue" {
  name   = "kms-policy${var.tag_postfix}"
  role   = "${aws_iam_role.glue_ETL_role.id}"
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


resource "aws_glue_job" "glue_job" {
  name     = "glue-ETL${var.tag_postfix}"
  role_arn = aws_iam_role.glue_ETL_role.arn

  command {
    script_location = "s3://${var.app_data_input_bucket_id}/semi-automated-data-ingestion/glue_ETL.py"
  }
  execution_property {
    max_concurrent_runs = 10
  }
}
