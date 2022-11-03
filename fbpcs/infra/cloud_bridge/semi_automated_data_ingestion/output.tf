output "semi_automated_glue_job_arn" {
  value       = aws_glue_job.glue_job.arn
  description = "The semi-automated Glue job ARN"
}
