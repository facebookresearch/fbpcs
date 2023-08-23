## Create the advertiser side catch-all s3 logging bucket for cloudtrail to store logs from all PL related s3 buckets
resource "aws_s3_bucket" "s3_logging_bucket" {
  bucket        = var.s3_logging_bucket_name
  force_destroy = true
}

## Create the S3 logging bucket policy to cloudtrail to read and write to the logging bucket
resource "aws_s3_bucket_policy" "s3_logging_bucket_policy" {
  bucket = aws_s3_bucket.s3_logging_bucket.id
  policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailAclCheck",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "${aws_s3_bucket.s3_logging_bucket.arn}"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "${aws_s3_bucket.s3_logging_bucket.arn}/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
POLICY
}
