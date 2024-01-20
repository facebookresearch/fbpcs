variable "region" {
  description = "region of the advertiser aws resources"
  default     = "us-west-2"
}

variable "installation_tag" {
  type        = string
  description = "Name of the TEE-PL advertiser infra installation tag"
  default     = "default-installation-tag"
}

variable "kinesis_log_stream_name" {
  type        = string
  description = "Name of the kinesys stream where various cloudwatch log groups (s3, Lambda, KMS etc.) in TEE-PL advertiser infra would push logs"
  default     = "kinesis-log-stream-advertiser"
}
