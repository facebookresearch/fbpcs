output "measurement_validation_agent_lambda_name" {
  value       = aws_lambda_function.measurement_validation_agent_lambda.function_name
  description = "Measurement validation agent lambda function name."
}
