resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true

  tags = {
    Name = "onedocker-vpc${var.tag_postfix}"
  }
}

# this will return all available zones in the region
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  az_names = data.aws_availability_zones.available.names
  # newbits will be used by cidrsubnet function. Reference: https://www.terraform.io/docs/language/functions/cidrsubnet.html
  # Here we set the value to 20 so the cidr could be x.x.x.x/20 that has 4096 hosts
  cidr_newbits = 20 - tonumber(split("/", var.vpc_cidr)[1])
}

resource "aws_subnet" "subnets" {
  for_each                = { for idx, az_name in local.az_names : idx => az_name }
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, local.cidr_newbits, each.key)
  availability_zone       = local.az_names[each.key]
  map_public_ip_on_launch = true
  tags = {
    Name = "onedocker-subnet${var.tag_postfix}"
  }
}

resource "aws_internet_gateway" "default" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "onedocker-igw${var.tag_postfix}"
  }
}

resource "aws_route" "internet_access" {
  route_table_id         = aws_vpc.main.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.default.id
}

resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.main.id
  dynamic "ingress" {
    for_each = var.ingress_rules
    content {
      description = lookup(ingress.value, "description", null)
      from_port   = lookup(ingress.value, "from_port", null)
      to_port     = lookup(ingress.value, "to_port", null)
      protocol    = lookup(ingress.value, "protocol", null)
      cidr_blocks = lookup(ingress.value, "cidr_blocks", null)
    }
  }

  ingress {
    description = "allow local traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "Open ports 5000-15500 to other party VPC"
    from_port   = 5000
    to_port     = 15500
    protocol    = "tcp"
    cidr_blocks = [var.otherparty_vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "onedocker-security-group${var.tag_postfix}"
  }
}
