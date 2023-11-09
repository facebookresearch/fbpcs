variable "region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "data_bucket_name" {
  description = "The name of the S3 bucket for advertisers to upload data"
  default     = ""
}

variable "semi_automated_lambda_arn" {
  description = "The ARN of the semi-automated pipeline lambda"
  default     = ""
}

variable "data_upload_key_path" {
  description = "S3 key path where events data will be uploaded"
  default     = "semi-automated-data-ingestion"
}
