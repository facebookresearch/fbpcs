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

resource "aws_subnet" "subnet0" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.subnet0_cidr
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "onedocker-subnet-0${var.tag_postfix}"
  }
}

resource "aws_subnet" "subnet1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.subnet1_cidr
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "onedocker-subnet-1${var.tag_postfix}"
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
