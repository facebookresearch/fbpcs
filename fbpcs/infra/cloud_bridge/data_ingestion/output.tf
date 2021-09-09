output "data_processing_output_bucket_id" {
  value       = aws_s3_bucket.bucket.id
  description = "The id of S3 bucked used to store data processing outputs"
}

output "data_processing_output_bucket_arn" {
  value       = aws_s3_bucket.bucket.arn
  description = "The arn of S3 bucked used to store data processing outputs"
}
