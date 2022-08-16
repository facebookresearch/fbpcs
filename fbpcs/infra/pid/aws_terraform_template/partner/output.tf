output "mrpid_partner_sfn_arn" {
  value       = aws_sfn_state_machine.mrpid_partner_sfn.arn
  description = "Generated partner Step Functions ARN"
}
