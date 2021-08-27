#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e
# shellcheck disable=SC1091
source ./util.sh

usage() {
    echo "Usage: deploy.sh <deploy|undeploy>
        [ -t, --tag | Tag that going to be appended after the resource name]
        [ -r, --region | AWS region, e.g. us-west-2 ]
        [ -a, --account_id | Publisher's AWS account ID]
        [ -p, --partner_account_id | Partner's AWS account ID]
        [ -c, --publisher_vpc_cidr | Publisher's VPC CIDR]
        [ -v, --partner_vpc_cidr | Partner's VPC CIDR]
        [ -b, --bucket | S3 bucket name for storing tfstate]"
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
        -t|--tag) tag_postfix="$2" ;;
        -r|--region) region="$2" ;;
        -a|--account_id) aws_account_id="$2" ;;
        -p|--partner_account_id) partner_aws_account_id="$2" ;;
        -c|--publisher_vpc_cidr) vpc_cidr="$2" ;;
        -v|--partner_vpc_cidr) partner_vpc_cidr="$2" ;;
        -b|--bucket) s3_bucket_for_storage="$2" ;;
        *) usage ;;
    esac
    shift
    shift
done

echo "AWS region is $region."
echo "The string '$tag_postfix' will be appended after the tag of the AWS resources."
echo "Publisher's AWS acount ID is $aws_account_id"
echo "Publisher's VPC CIDR is $vpc_cidr"
echo "Partner's AWS account ID is $partner_aws_account_id"
echo "Partner's VPC CIDR is $partner_vpc_cidr"
echo "The S3 bucket for storing the Terraform state file is $s3_bucket_for_storage"

undeploy_aws_resources () {
    echo "Start undeploying..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/pce$tag_postfix.tfstate" "$aws_account_id"
    echo "All tfstate files exist. Continue..."

    echo "########################Delete PCE resources########################"
    cd /terraform_deployment/terraform_scripts/common/pce
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pce$tag_postfix.tfstate"
    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix"
}

deploy_aws_resources () {
    echo "########################Started AWS Infrastructure Deployment########################"
    create_s3_bucket "$s3_bucket_for_storage" "$region" "$aws_account_id"

    # Deploy PCE Terraform scripts
    cd /terraform_deployment/terraform_scripts/common/pce
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pce$tag_postfix.tfstate"
    terraform apply \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "vpc_cidr=$vpc_cidr" \
        -var "otherparty_vpc_cidr=$partner_vpc_cidr"

    # Print the output
    echo "######################## PCE terraform output ########################"
    terraform output

    echo "########################Finished AWS Infrastructure Deployment########################"
}

##########################################
# Main
##########################################

if "$undeploy"
then
    echo "Undeploying the AWS resources..."
    undeploy_aws_resources
else
    deploy_aws_resources
fi
exit 0
