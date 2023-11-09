output "semi_automated_glue_job_arn" {
  value       = aws_glue_job.glue_job.arn
  description = "The semi-automated Glue job ARN"
}

output "semi_automated_lambda_arn" {
  value       = aws_lambda_function.lambda_trigger.arn
  description = "The semi-automated Lambda job ARN"
}
