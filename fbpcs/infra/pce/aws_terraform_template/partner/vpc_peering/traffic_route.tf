resource "aws_route" "peer_connection_access" {
  route_table_id            = var.route_table_id
  destination_cidr_block    = var.destination_cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.vpc_peering_conn.id
}
