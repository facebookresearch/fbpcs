output "data_processing_output_bucket_id" {
  value       = aws_s3_bucket.bucket.id
  description = "The id of S3 bucked used to store data processing outputs"
}

output "data_processing_output_bucket_arn" {
  value       = aws_s3_bucket.bucket.arn
  description = "The arn of S3 bucked used to store data processing outputs"
}

output "firehose_stream_name" {
  value       = aws_kinesis_firehose_delivery_stream.extended_s3_stream.name
  description = "The Kinesis firehose stream name"
}

output "data_ingestion_kms_key" {
  value       = aws_kms_key.s3_kms_key.id
  description = "The data bucket KMS key"
}
