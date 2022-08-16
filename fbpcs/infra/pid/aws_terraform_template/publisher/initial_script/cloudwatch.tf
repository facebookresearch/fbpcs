resource "aws_cloudwatch_log_group" "mrpid_publisher_sfn_log_group" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_sfn_log_group"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "mrpid_publisher_ec2_log_group" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_ec2_log_group"
  retention_in_days = 30
}
