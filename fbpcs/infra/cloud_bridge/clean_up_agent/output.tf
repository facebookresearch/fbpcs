output "clean_up_agent_lambda_iam_role_arn" {
  value       = aws_iam_role.clean_up_agent_lambda_iam.arn
  description = "Clean up agent lambda IAM role ARN."
}
