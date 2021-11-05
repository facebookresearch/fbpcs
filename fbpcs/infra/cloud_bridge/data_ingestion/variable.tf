variable "region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "data_processing_output_bucket" {
  description = "Data output bucket for Kinesis Firehose stream"
  default     = ""
}

variable "data_processing_lambda_s3_bucket" {
  description = "Source S3 bucket for data processing lambda deployment file"
  default     = ""
}

variable "data_processing_lambda_s3_key" {
  description = "Source S3 key for data processing lambda deployment file"
  default     = ""
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id, that's used to read encrypted S3 files"
  default     = ""
}

variable "data_upload_key_path" {
  description = "your aws account id, that's used to read encrypted S3 files"
  default     = "semi-automated-data-ingestion"
}
