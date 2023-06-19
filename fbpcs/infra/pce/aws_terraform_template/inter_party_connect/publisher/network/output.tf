output "transit_gateway_id" {
  value       = aws_ec2_transit_gateway.interparty_transit_gateway.id
  description = "The id of the inter-party transit gateway"
}
