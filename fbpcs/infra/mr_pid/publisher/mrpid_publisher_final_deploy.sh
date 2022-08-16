#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e
# shellcheck disable=SC1091
source ./util.sh

usage() {
    echo "Note: This final script needs to be run after partner side deployment script!"
    echo "Usage: mrpid_publisher_final_deploy.sh <deploy|undeploy>
        [ -t, --tag | A unique identifier to identify resources in this MR-PID deployment, please use the same tag from initial script.]
        [ -r, --region | MR-PID Publisher AWS region, e.g. us-west-2 ]
        [ -a, --account_id | MR-PID Publisher AWS account ID]
        [ -p, --partner_account_id | MR-PID Partner AWS account ID]
        [ -b, --bucket | optional. S3 bucket name for storing configs: tfstate]"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

undeploy=false
case "$1" in
    deploy) ;;
    undeploy) undeploy=true ;;
    *) usage ;;
esac
shift

while [ $# -gt 0 ]; do
    case "$1" in
        -t|--tag) pid_id="$2" ;;
        -r|--region) region="$2" ;;
        -a|--account_id) aws_account_id="$2" ;;
        -p|--partner_account_id) partner_account_id="$2" ;;
        -b|--bucket) s3_bucket_for_storage="$2" ;;
        *) usage ;;
    esac
    shift
    shift
done

#### Terraform Logs
if [ -z ${TF_LOG+x} ]; then
    echo "Terraform Detailed Error Logging Disabled"
else
    echo "Terraform Log Level: $TF_LOG"
    echo "Terraform Log File: $TF_LOG_PATH"
    echo "Terraform Log File: $TF_LOG_STREAMING"
    echo
fi

undeploy_aws_resources () {
    input_validation "$region" "$pid_id" "$aws_account_id" "$partner_account_id" "$s3_bucket_for_storage"
    echo "Start undeploying MR-PID Publisher resources..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/pid$tag_postfix.tfstate" "$aws_account_id"
    echo "All tfstate files exist. Continue..."

    md5hash_partner_account_id=$(echo -n $partner_account_id | md5sum | awk '{print $1}')

    echo "########################Delete MR-PID resources########################"
    cd /terraform_deployment/terraform_scripts_final
    terraform init \
        -reconfigure \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pid$tag_postfix.tfstate"
    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "pid_id=$pid_id" \
        -var "partner_account_id=$partner_account_id" \
        -var "md5hash_partner_account_id=$md5hash_partner_account_id"
}

deploy_aws_resources () {
    input_validation "$region" "$pid_id" "$aws_account_id" "$partner_account_id" "$s3_bucket_for_storage"
    # Clean up previously generated resources if any
    cleanup_generated_resources
    echo "########################Started MR-PID AWS Infrastructure Deployment########################"
    echo "creating s3 bucket, if it does not exist"
    validate_or_create_s3_bucket "$s3_bucket_for_storage" "$region" "$aws_account_id"

    md5hash_partner_account_id=$(echo -n $partner_account_id | md5sum | awk '{print $1}')

    # Deploy MR-PID Publisher PID Terraform scripts
    cd /terraform_deployment/terraform_scripts_final
    terraform init \
        -reconfigure \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pid$tag_postfix.tfstate"
    terraform apply \
        -auto-approve \
        -var "aws_region=$region" \
        -var "pid_id=$pid_id" \
        -var "partner_account_id=$partner_account_id" \
        -var "md5hash_partner_account_id=$md5hash_partner_account_id"

    echo "########################Finished MR-PID AWS Infrastructure Deployment########################"
}

##########################################
# Main
##########################################

tag_postfix="-${pid_id}-final"

# if no input for bucket names, then go by default

if [ -z ${s3_bucket_for_storage+x} ]
then
    # s3_bucket_for_storage is unset
    s3_bucket_for_storage="fb-pc-mrpid-publisher-config$tag_postfix"
else
    # s3_bucket_for_storage is set, but add tags to it
    s3_bucket_for_storage="$s3_bucket_for_storage$tag_postfix"
fi

echo "MR-PID Publisher AWS region is $region."
echo "MR-PID Publisher AWS acount ID is $aws_account_id"
echo "MR-PID Partner AWS acount ID is $partner_account_id"
echo "The S3 bucket for storing the Terraform state file is $s3_bucket_for_storage and it is in region $region"

if "$undeploy"
then
    echo "Undeploying the MR-PID Publisher AWS resources..."
    undeploy_aws_resources
else
    deploy_aws_resources
fi
exit 0
