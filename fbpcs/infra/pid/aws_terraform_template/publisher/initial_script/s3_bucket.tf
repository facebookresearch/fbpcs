resource "aws_s3_bucket" "mrpid_publisher_intermediate_bucket" {
  bucket = "mrpid-publisher-${var.pce_instance_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "mrpid_block_public_access" {
  bucket = aws_s3_bucket.mrpid_publisher_intermediate_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "mrpid_publisher_confs_bucket" {
  bucket = "mrpid-publisher-${var.pce_instance_id}-confs"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "mrpid_confs_block_public_access" {
  bucket = aws_s3_bucket.mrpid_publisher_confs_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_object" "cloudwatch_agent_install_script" {
  bucket = aws_s3_bucket.mrpid_publisher_confs_bucket.id

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
            "file_path": "/mnt/var/log/spark/PubStageOneConsole.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_one_console_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PubStageTwoConsole.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_two_console_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PubStageThreeConsole.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_three_console_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PubStageOneYarn.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_one_yarn_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PubStageTwoYarn.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_two_yarn_log",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/spark/PubStageThreeYarn.log",
            "log_group_name": "mrpid-publisher-ec2-log-group-${var.pce_instance_id}",
            "log_stream_name": "publisher_stage_three_yarn_log",
            "timezone": "UTC"
          }
        ]
      }
    },
    "log_stream_name": "publisher_spark_log",
    "force_flush_interval" : 10
  }
}
EOT

sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

echo "Start cloudwatch agent"
EOF
}
