resource "aws_ecs_cluster" "main" {
  name = "onedocker-cluster${var.tag_postfix}"

  tags = {
    pce-tag = "cluster${var.tag_postfix}"
  }

  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
  }

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}
