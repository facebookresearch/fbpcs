variable "region" {
  description = "region of the advertiser aws resources"
  default     = "us-west-2"
}

variable "installation_tag" {
  type        = string
  description = "Name of the TEE-PL advertiser infra installation tag"
  default     = "default-installation-tag"
}

variable "s3_logging_bucket_name" {
  type        = string
  description = "Name of the S3 bucket where all logs generated from TEE-PL advertiser side KMS cloudtrail logs will be stored"
  default     = "s3-log-bucket-advertiser"
}

variable "kinesis_log_stream_name" {
  type        = string
  description = "Name of the kinesys stream where various cloudwatch log groups (s3, Lambda, KMS etc.) in TEE-PL advertiser infra would push logs"
  default     = "kinesis-log-stream-advertiser"
}
