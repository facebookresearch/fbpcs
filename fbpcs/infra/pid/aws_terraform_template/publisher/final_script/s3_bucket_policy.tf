resource "aws_s3_bucket_policy" "mrpid_allow_read_access_from_partner_account" {
  bucket = "mrpid-publisher-${var.md5hash_partner_account_id}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["arn:aws:iam::${var.partner_account_id}:role/mrpid_partner_ec2_role", "arn:aws:iam::${var.partner_account_id}:role/mrpid_partner_sfn_role"]
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}/*",
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}"
      ]
    },
    {
      "Effect": "Deny",
      "Principal": {
        "AWS": "*"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}/*",
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}"
      ],
      "Condition": {
        "Bool": {
          "aws:SecureTransport": "false"
        }
      }
    },
    {
      "Effect": "Deny",
      "Principal": {
        "AWS": "*"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}/*",
        "arn:aws:s3:::mrpid-publisher-${var.md5hash_partner_account_id}"
      ],
      "Condition": {
        "NumericLessThan": {
          "s3:TlsVersion": "1.2"
        }
      }
    }
  ]
}
EOF
}
