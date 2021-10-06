# Manual upload Data Validation
This data validation lambda job is triggered when a CSV file is uploaded to the configured S3 bucket key at `s3://<upload_and_validation_s3_bucket>/<events_data_upload_s3_key>`.
The job first checks if there is a valid header row, and if it finds one it then validates all of the rows.
It then outputs the validation result to a file under the configured S3 key at `s3://<upload_and_validation_s3_bucket>/<validation_results_s3_key>`.

Validation file output format example:
```
Validation Summary:
Total rows: 27
Valid rows: 3
Rows with errors: 24
Line numbers missing 'currency_type': 14,22,23,24,25,26,27

Line numbers missing 'conversion_value': 15,16,17,22,23,24,25,26,27

Line numbers missing 'event_type': 18,22,23,24,25,26

Line numbers missing 'timestamp': 19,20,21,22,23,24,25,26,27

Line numbers missing 'action_source': 22,23,24,25,26,27

Line numbers that are missing 1 or more of these required fields 'device_id,email': 13,22,23,24,25,26,27
Line numbers with incorrect 'action_source' format: 3,4

Line numbers with incorrect 'event_type' format: 5,6

Line numbers with incorrect 'conversion_value' format: 7,8

Line numbers with incorrect 'currency_type' format: 9

Line numbers with incorrect 'timestamp' format: 10

Line numbers with incorrect 'email' format: 11,14

Line numbers with incorrect 'device_id' format: 14
```

## Deployment Requirements
* Configured AWS CLI
  * `aws configure`
* Terraform CLI
* S3 bucket
* 2 different S3 keys: 1 for upload, 1 for storing the validation output
* Tag postfix: a string added to the end of each created resource to avoid resource name collisions

## Deployment
```
terraform apply \
    -auto-approve \
    -var "aws_region=<aws_region>" \
    -var "tag_postfix=<tag_postfix>" \
    -var "aws_account_id=<aws_account_id>" \
    -var "upload_and_validation_s3_bucket=<bucket-name>" \
    -var "events_data_upload_s3_key=<upload-key>" \
    -var "validation_results_s3_key=<validation-results-key>"
```
Note:
* Do not use a long tag_postfix, otherwise you may hit the 64 character lambda name limit
* Add optional argument '-var "validation_debug_mode=1"' to debug

## Testing after deployment
```
aws s3 cp test1.csv s3://<bucket-name>/<upload-key>/test1.csv
aws s3 ls s3://<bucket-name>/<validation-results-key>/
```
### Copy the validation output file path, then check the results
```
aws s3 cp s3://<bucket-name>/<validation-results-key>/test1.csv_validation-results_2021-10-04T19:58:53.528348 -
```

## Usage
Uploading 1 file at a time is recommended, so that the lambda processor does not time out while processing. Also the uploaded CSV files should have fewer than 100 million rows.

## Delete deployment
Using the same params that were used to create the deployment, run `terraform destroy`:
```
terraform destroy \
    -auto-approve \
    -var "aws_region=<aws_region>" \
    -var "tag_postfix=<tag_postfix>" \
    -var "aws_account_id=<aws_account_id>" \
    -var "upload_and_validation_s3_bucket=<bucket-name>" \
    -var "events_data_upload_s3_key=<upload-key>" \
    -var "validation_results_s3_key=<validation-results-key>"
```
