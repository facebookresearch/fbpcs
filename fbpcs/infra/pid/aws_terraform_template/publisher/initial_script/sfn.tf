resource "aws_sfn_state_machine" "mrpid_publisher_sfn" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_sfn"

  role_arn = aws_iam_role.mrpid_publisher_sfn_role.arn

  type = "STANDARD"

  definition = data.template_file.publisher_sfn_definition.rendered

  logging_configuration {
    log_destination = "${aws_cloudwatch_log_group.mrpid_publisher_sfn_log_group.arn}:*"
    include_execution_data = true
    level = "ALL"
  }
}
