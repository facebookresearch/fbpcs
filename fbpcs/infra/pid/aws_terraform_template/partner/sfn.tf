resource "aws_sfn_state_machine" "mrpid_partner_sfn" {
  name = "mrpid_partner_sfn"

  role_arn = aws_iam_role.mrpid_partner_sfn_role.arn

  type = "STANDARD"

  definition = data.template_file.partner_sfn_definition.rendered

  logging_configuration {
    log_destination = "${aws_cloudwatch_log_group.mrpid_partner_sfn_log_group.arn}:*"
    include_execution_data = true
    level = "ALL"
  }
}
