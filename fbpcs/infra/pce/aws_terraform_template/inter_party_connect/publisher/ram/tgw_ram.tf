resource "aws_ram_resource_share" "tgw_resource_share" {
  name                      = "transit_gateway_resource_share${var.tag_postfix}"
  allow_external_principals = true
}

resource "aws_ram_resource_association" "aws_ram_resource_tgw_association" {
  resource_arn       = var.transit_gateway_arn
  resource_share_arn = aws_ram_resource_share.tgw_resource_share.arn
}

resource "aws_ram_principal_association" "aws_ram_principal_tgw_association" {
  principal          = var.principal
  resource_share_arn = aws_ram_resource_share.tgw_resource_share.arn
}
