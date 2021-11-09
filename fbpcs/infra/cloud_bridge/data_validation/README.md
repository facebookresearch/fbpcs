# Manual-upload and semi-automated Data Validation
This data validation lambda job is triggered when a CSV file is uploaded to the configured S3 bucket key at `s3://<upload_and_validation_s3_bucket>/<events_data_upload_s3_key or semi_automated_key_path>`.
The job first checks if there is a valid header row, and if it finds one it then validates all of the rows.
It then outputs the validation result to a file under the configured S3 key at `s3://<upload_and_validation_s3_bucket>/<events_data_upload_s3_key or semi_automated_key_path>/<validation_results_s3_key>`.
If no <validation_results_s3_key> is specified, the validation result will be stored in the default key:
* /<events_data_upload_s3_key or semi_automated_key_path>

Validation file output format examples:

Semi-automated:
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

Private Attribution:
```
Validation Summary:
Total rows: 20
Valid rows: 14
Rows with errors: 6
Line numbers missing 'id_': 5,7

Line numbers missing 'conversion_timestamp': 8

Line numbers with incorrect 'conversion_metadata' format: 10

Line numbers with incorrect 'id_' format: 11

Line numbers with incorrect 'conversion_value' format: 12
```

Private Lift:
```
Validation Summary:
Total rows: 100
Valid rows: 70
Rows with errors: 30
Line numbers missing 'id_': 7,8,9,10,11,12,13,34,35,36,37

Line numbers missing 'event_timestamp': 46,47,48,63,64

Line numbers missing 'value': 76,83,84,85

Line numbers with incorrect 'event_timestamp' format: 19,53

Line numbers with incorrect 'id_' format: 23,24,25,26,47,48,58,75,76

Line numbers with incorrect 'value' format: 56,93
```

## Deployment Requirements
* Configured AWS CLI
  * `aws configure`
* Terraform CLI
* S3 bucket
* S3 keys where the CSV files will be uploaded
* Tag postfix: a string added to the end of each created resource to avoid resource name collisions

## Optional variables
* validation_results_s3_key
* validation_debug_mode

## Deployment
```
s3_bucket_for_storage=<your-config-bucket-name>
region=<aws-region>
tag_postfix=<tag-postfix>
aws_account_id=<your-aws-account-id>
s3_bucket_data_pipeline=<your-data-bucket-name>

events_data_upload_s3_key="events-data-validation"
semi_automated_key_path="semi-automated-data-ingestion"

terraform init -reconfigure \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/data_validation$tag_postfix.tfstate"

terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "aws_account_id=$aws_account_id" \
    -var "upload_and_validation_s3_bucket=$s3_bucket_data_pipeline" \
    -var "events_data_upload_s3_key=$events_data_upload_s3_key" \
    -var "semi_automated_key_path=$semi_automated_key_path"
```
Note:
* Do not use a long tag_postfix, otherwise you may hit the 64 character lambda name limit
* Add optional argument '-var "validation_debug_mode=1"' to debug

## Testing the events_upload validation after deployment
```
aws s3 cp test1.csv s3://$s3_bucket_data_pipeline/$events_data_upload_s3_key/test1.csv
aws s3 ls s3://$s3_bucket_data_pipeline/$events_data_upload_s3_key/
```

## Testing the semi_automated validation after deployment
```
aws s3 cp test1.csv s3://$s3_bucket_data_pipeline/$semi_automated_key_path/test1.csv
aws s3 ls s3://$s3_bucket_data_pipeline/$semi_automated_key_path/
```

## Usage
Uploading 1 file at a time is recommended, so that the lambda processor does not time out while processing. Also the uploaded CSV files should have fewer than 100 million rows.

## Delete deployment
Using the same params that were used to create the deployment, run `terraform destroy`:
```
terraform destroy \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "aws_account_id=$aws_account_id" \
    -var "upload_and_validation_s3_bucket=$s3_bucket_data_pipeline" \
    -var "events_data_upload_s3_key=$events_data_upload_s3_key" \
    -var "semi_automated_key_path=$semi_automated_key_path"
```
