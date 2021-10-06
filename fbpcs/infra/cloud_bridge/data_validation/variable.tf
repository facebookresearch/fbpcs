variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id, that's used to read encrypted S3 files"
  default     = ""
}

variable "upload_and_validation_s3_bucket" {
  description = "the bucket where manually preprocessed event CSVs will be uploaded, and where the validation results will be stored"
  default     = ""
}

variable "events_data_upload_s3_key" {
  description = "the object key where manually preprocessed event CSVs will be uploaded"
  default     = ""
}

variable "validation_results_s3_key" {
  description = "the bucket path where validation results will be stored, this field is optional"
  default     = ""
}

variable "validation_debug_mode" {
  description = "extra logging for debugging/dev purpose"
  default     = 0
}
