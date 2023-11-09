output "ingestion_input_data_validation_lambda_arn" {
  value       = aws_lambda_function.ingestion_input_data_validation_lambda.arn
  description = "The ingestion input data validation Lambda job ARN"
}
