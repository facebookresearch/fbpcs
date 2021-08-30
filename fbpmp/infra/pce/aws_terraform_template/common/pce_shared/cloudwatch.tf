resource "aws_cloudwatch_log_group" "onedocker_cloudwatch_log_group" {
  name = "/ecs/onedocker-container${var.tag_postfix}"
  tags = {
    "pce:pce-id" = var.pce_id
  }
}
