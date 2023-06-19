resource "aws_ec2_transit_gateway" "interparty_transit_gateway" {
  description = "transit gateway for inter-party network connectivity"

  auto_accept_shared_attachments = "enable"
  tags = {
    Name = "onedocker-tgw${var.tag_postfix}"
  }
}
