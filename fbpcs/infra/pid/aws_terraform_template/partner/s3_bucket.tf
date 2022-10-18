resource "aws_s3_bucket" "mrpid_partner_intermediate_bucket" {
  bucket = "mrpid-partner-${var.md5hash_aws_account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "mrpid_block_public_access" {
  bucket = aws_s3_bucket.mrpid_partner_intermediate_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "mrpid_allow_read_access_from_publisher_account" {
  bucket = aws_s3_bucket.mrpid_partner_intermediate_bucket.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["arn:aws:iam::${var.publisher_account_id}:role/mrpid-publisher-ec2-role-${var.md5hash_aws_account_id}", "arn:aws:iam::${var.publisher_account_id}:role/mrpid-publisher-sfn-role-${var.md5hash_aws_account_id}"]
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}/*",
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}"
      ]
    },
    {
      "Effect": "Deny",
      "Principal": {
        "AWS": "*"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}/*",
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}"
      ],
      "Condition": {
        "Bool": {
          "aws:SecureTransport": "false"
        }
      }
    },
    {
      "Effect": "Deny",
      "Principal": {
        "AWS": "*"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}/*",
        "arn:aws:s3:::mrpid-partner-${var.md5hash_aws_account_id}"
      ],
      "Condition": {
        "NumericLessThan": {
          "s3:TlsVersion": "1.2"
        }
      }
    }
  ]
}
EOF
}

resource "aws_s3_bucket" "mrpid_partner_confs_bucket" {
  bucket = "mrpid-partner-${var.md5hash_aws_account_id}-confs"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "mrpid_confs_block_public_access" {
  bucket = aws_s3_bucket.mrpid_partner_confs_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_object" "cloudwatch_agent_install_script" {
  bucket = aws_s3_bucket.mrpid_partner_confs_bucket.id

  key = "cloudwatch_agent/cloudwatch_agent_install.sh"

  content = <<EOF
#!/bin/bash

if grep isMaster /mnt/var/lib/info/instance.json | grep false;
then
    echo "This is not master node, do nothing,exiting"
    exit 0
fi
echo "This is master, continuing to execute script"

sudo yum -y install amazon-cloudwatch-agent

sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json > /dev/null <<EOT
{
  "agent": {
    "metrics_collection_interval": 20,
    "logfile": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log"
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/mnt/var/log/spark/PartnerStageOneConsole.log",
            "log_group_name": "mrpid-partner-ec2-log-group",
            "log_stream_name": "partner_stage_one_console_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PartnerStageTwoConsole.log",
            "log_group_name": "mrpid-partner-ec2-log-group",
            "log_stream_name": "partner_stage_two_console_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PartnerStageOneYarn.log",
            "log_group_name": "mrpid-partner-ec2-log-group",
            "log_stream_name": "partner_stage_one_yarn_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PartnerStageTwoYarn.log",
            "log_group_name": "mrpid-partner-ec2-log-group",
            "log_stream_name": "partner_stage_two_yarn_log",
            "timezone": "UTC"
          }
        ]
      }
    },
    "log_stream_name": "partner_spark_log",
    "force_flush_interval" : 10
  }
}
EOT

sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

echo "Start cloudwatch agent"
EOF
}
