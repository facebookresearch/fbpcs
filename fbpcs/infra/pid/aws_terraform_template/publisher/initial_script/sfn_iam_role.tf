resource "aws_iam_role" "mrpid_publisher_sfn_role" {
  name = "mrpid-publisher-sfn-role-${var.pce_instance_id}"

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
  name = "mrpid-sfn-access-cloudwatch-logs-policy-${var.pce_instance_id}"

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
  name = "mrpid-sfn-access-emr-policy-${var.pce_instance_id}"

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
