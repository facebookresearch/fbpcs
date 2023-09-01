variable "region" {
  description = "region of the advertiser aws resources"
  default     = "us-west-2"
}

variable "lambda_name" {
  type        = string
  description = "Name of the lambda  used in execution of TEE-PL"
}

variable "kinesis_log_stream_name" {
  type        = string
  description = "Name of the kinesys stream where various cloudwatch log groups (s3, Lambda etc.) would push logs"
  default     = "kinesis-log-stream-advertiser"
}

variable "should_create_log_group" {
  type        = bool
  description = "Create cloudWatch log group for the lambda"
  default     = false
}
