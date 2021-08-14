provider "aws" {
  profile = "default"
  region  = var.aws_region
}

terraform {
  backend "s3" {}
}

data "aws_arn" "ecs_task_execution_role_arn" {
  arn = "arn:aws:iam::${var.aws_account_id}:role/onedocker-ecs-task-execution-role${var.tag_postfix}"
}

data "aws_arn" "ecs_task_role" {
  arn = "arn:aws:iam::${var.aws_account_id}:role/onedocker-ecs-task-role${var.tag_postfix}"
}

resource "aws_ecs_task_definition" "onedocker_task_def" {
  family                   = "onedocker-task${var.tag_postfix}"
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 30720
  task_role_arn            = data.aws_arn.ecs_task_role.arn
  execution_role_arn       = data.aws_arn.ecs_task_execution_role_arn.arn
  requires_compatibilities = ["FARGATE"]
  container_definitions    = <<DEFINITION
[
  {
    "name": "onedocker-container${var.tag_postfix}",
    "essential": true,
    "cpu": 4096,
    "memory": 30720,
    "image": "${var.onedocker_ecs_container_image}",
    "entryPoint": ["sh","-c"],
    "logConfiguration": {
		"logDriver": "awslogs",
		"options": {
			"awslogs-group": "${aws_cloudwatch_log_group.onedocker_cloudwatch_log_group.name}",
			"awslogs-region": "${var.aws_region}",
			"awslogs-stream-prefix": "ecs"
			}
  	}
  }
]
DEFINITION
}
