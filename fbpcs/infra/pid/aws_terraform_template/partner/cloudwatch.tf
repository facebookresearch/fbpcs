resource "aws_cloudwatch_log_group" "mrpid_partner_sfn_log_group" {
  name = "mrpid_partner_sfn_log_group"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "mrpid_partner_ec2_log_group" {
  name = "mrpid_partner_ec2_log_group"
  retention_in_days = 30
}
