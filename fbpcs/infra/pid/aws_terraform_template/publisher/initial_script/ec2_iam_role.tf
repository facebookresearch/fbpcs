resource "aws_iam_role" "mrpid_publisher_ec2_role" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_ec2_role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_instance_profile" "mrpid_publisher_ec2_role" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_ec2_role"
  role = aws_iam_role.mrpid_publisher_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "mrpid_emr_ec2_role_attach" {
  role = aws_iam_role.mrpid_publisher_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_role_policy" "mrpid_ec2_access_partner_s3_policy" {
  name = "mrpid_${var.md5hash_partner_account_id}_ec2_access_partner_s3_policy"

  role = aws_iam_role.mrpid_publisher_ec2_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::mrpid-partner-${var.md5hash_partner_account_id}/*",
        "arn:aws:s3:::mrpid-partner-${var.md5hash_partner_account_id}"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "mrpid_ec2_access_cloudwatch_logs_policy" {
  name = "mrpid_${var.md5hash_partner_account_id}_ec2_access_cloudwatch_logs_policy"

  role = aws_iam_role.mrpid_publisher_ec2_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:DescribeLogStreams",
        "logs:PutLogEvents",
        "logs:GetLogEvents"
      ],
      "Resource": "${aws_cloudwatch_log_group.mrpid_publisher_ec2_log_group.arn}:*"
    }
  ]
}
EOF
}
