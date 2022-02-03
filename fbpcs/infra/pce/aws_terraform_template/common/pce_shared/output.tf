output "onedocker_task_definition_family" {
  value       = aws_ecs_task_definition.onedocker_task_def.family
  description = "The family of the onedocker task definition"
}

output "onedocker_task_definition_revision" {
  value       = aws_ecs_task_definition.onedocker_task_def.revision
  description = "The revision of the onedocker task definition"
}

output "onedocker_task_definition_container_definitons" {
  value       = aws_ecs_task_definition.onedocker_task_def.container_definitions
  description = "The container definitions in the onedocker task definition"
}

output "ecs_task_execution_role_name" {
  value       = aws_iam_role.onedocker_ecs_task_execution_role.name
  description = "The ECS task execution IAM role name"
}
