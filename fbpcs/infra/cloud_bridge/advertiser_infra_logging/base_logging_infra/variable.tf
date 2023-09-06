variable "region" {
  description = "region of the advertiser aws resources"
  default     = "us-west-2"
}

variable "s3_logging_bucket_name" {
  type        = string
  description = "Name of the S3 bucket where all logs generated from other S3 buckets will be stored"
  default     = "s3-log-bucket-advertiser"
}

variable "kinesis_log_stream_name" {
  type        = string
  description = "Name of the kinesys stream where various cloudwatch log groups (s3, Lambda etc.) would push logs"
  default     = "kinesis-log-stream-advertiser"
}

variable "kinesis_read_policy_name" {
  type        = string
  description = "Name of the kinesis read policy name"
  default     = "kinesis-read-policy"
}

variable "kinesis_read_role_name" {
  type        = string
  description = "Name of the kinesis read role name"
  default     = "kinesis-read-role"
}

