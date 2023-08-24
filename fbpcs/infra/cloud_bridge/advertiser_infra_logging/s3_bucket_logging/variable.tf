variable "region" {
  description = "region of the advertiser aws resources"
  default     = "us-west-2"
}

variable "s3_bucket_name" {
  type        = string
  description = "Name of the data S3 bucket used in execution of TEE-PL"
}

variable "s3_logging_bucket_name" {
  type        = string
  description = "Name of the S3 bucket where all logs generated from other TEE-PL advertiser side S3 buckets will be stored"
  default     = "s3-log-bucket-advertiser"
}

variable "kinesis_log_stream_name" {
  type        = string
  description = "Name of the kinesys stream where various cloudwatch log groups (s3, Lambda etc.) in TEE-PL advertiser infra would push logs"
  default     = "kinesis-log-stream-advertiser"
}
