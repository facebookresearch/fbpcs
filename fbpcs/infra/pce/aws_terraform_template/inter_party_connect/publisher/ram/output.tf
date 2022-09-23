output "transit_gateway_resource_share_id" {
  value       = aws_ram_resource_share.tgw_resource_share.id
  description = "The id of the transit gateway resource share"
}
