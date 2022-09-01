output "firehose_stream_name" {
  value       = aws_kinesis_firehose_delivery_stream.extended_s3_stream.name
  description = "The Kinesis firehose stream name"
}
