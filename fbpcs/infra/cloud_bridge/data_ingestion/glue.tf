resource "aws_glue_catalog_database" "mpc_database" {
  name = "mpc-events-db${var.tag_postfix}"
}

resource "aws_iam_role" "glue_service_role" {
  name               = "glue-service-role${var.tag_postfix}"
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
  role       = aws_iam_role.glue_service_role.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_console_access" {
  role       = aws_iam_role.glue_service_role.id
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

resource "aws_iam_role_policy" "s3_policy" {
  name   = "s3-policy${var.tag_postfix}"
  role   = aws_iam_role.glue_service_role.id
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
        "arn:aws:s3:::${var.data_processing_output_bucket}",
        "arn:aws:s3:::${var.data_processing_output_bucket}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_glue_crawler" "mpc_events_crawler" {
  database_name = aws_glue_catalog_database.mpc_database.name
  name          = "mpc-events-crawler${var.tag_postfix}"
  role          = aws_iam_role.glue_service_role.arn

  configuration = jsonencode(
    {
      Grouping = {
        TableGroupingPolicy = "CombineCompatibleSchemas"
      }
      CrawlerOutput = {
        Partitions : { "AddOrUpdateBehavior" : "InheritFromTable" },
        Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
      }
      Version = 1
    }
  )

  s3_target {
    path = "s3://${var.data_processing_output_bucket}"
    exclusions = [
      "processing-failed**",
      "${var.data_upload_key_path}/**",
      "${var.query_results_key_path}/**"
    ]
  }

  schedule = "cron(0 * * * ? *)"

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}
