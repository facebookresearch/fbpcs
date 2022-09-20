output "firehose_stream_name" {
  value       = aws_kinesis_firehose_delivery_stream.extended_s3_stream.name
  description = "The Kinesis firehose stream name"
}

output "events_data_crawler_arn" {
  value       = aws_glue_crawler.mpc_events_crawler.arn
  description = "The events_data Glue crawler ARN"
}
