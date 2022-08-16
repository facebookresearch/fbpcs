resource "aws_iam_role" "mrpid_publisher_sfn_role" {
  name = "mrpid_publisher_${var.md5hash_partner_account_id}_sfn_role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "mrpid_sfn_access_cloudwatch_logs_policy" {
  name = "mrpid_${var.md5hash_partner_account_id}_sfn_access_cloudwatch_logs_policy"

  role = aws_iam_role.mrpid_publisher_sfn_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "mrpid_sfn_access_emr_policy" {
  name = "mrpid_${var.md5hash_partner_account_id}_sfn_access_emr_policy"

  role = aws_iam_role.mrpid_publisher_sfn_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:CancelSteps",
        "elasticmapreduce:RunJobFlow",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:TerminateJobFlows",
        "elasticmapreduce:SetTerminationProtection"
      ],
      "Resource": "arn:aws:elasticmapreduce:*:*:cluster/*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "${aws_iam_role.mrpid_publisher_emr_role.arn}",
        "${aws_iam_role.mrpid_publisher_ec2_role.arn}"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "mrpid_sfn_access_partner_s3_policy" {
  name = "mrpid_${var.md5hash_partner_account_id}_sfn_access_partner_s3_policy"

  role = aws_iam_role.mrpid_publisher_sfn_role.id

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
