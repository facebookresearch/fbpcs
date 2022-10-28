resource "aws_iam_policy" "mrpid_partner_minimal_running_policy" {
  name = "mrpid-partner-minimal-running-policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "states:DescribeExecution",
        "states:StartExecution"
      ],
      "Resource": "${aws_sfn_state_machine.mrpid_partner_sfn.arn}"
    }
  ]
}
EOF
}
