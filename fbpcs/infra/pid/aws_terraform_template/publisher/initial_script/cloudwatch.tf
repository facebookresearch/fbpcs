resource "aws_cloudwatch_log_group" "mrpid_publisher_sfn_log_group" {
  name = "mrpid-publisher-sfn-log-group-${var.pce_instance_id}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "mrpid_publisher_ec2_log_group" {
  name = "mrpid-publisher-ec2-log-group-${var.pce_instance_id}"
  retention_in_days = 30
}
