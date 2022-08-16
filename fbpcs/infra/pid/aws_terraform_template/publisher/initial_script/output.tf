output "mrpid_publisher_sfn_arn" {
  value       = aws_sfn_state_machine.mrpid_publisher_sfn.arn
  description = "Generated publisher Step Functions ARN"
}
