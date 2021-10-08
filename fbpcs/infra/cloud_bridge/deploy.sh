#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

usage() {
    echo "Usage: deploy.sh
        [ -r | AWS region, e.g. us-west-2 ]
        [ -t | Tag that going to use as the PCE id and appended after the resource name]
        [ -a | Your AWS account ID]
        [ -p | Publisher's AWS account ID]
        [ -v | Publisher's VPC Id]
        [ -s | S3 bucket name for storing tfstate/lambda function]
        [ -d | S3 bucket name for storing lambda processed results]
        [ -b | build manual upload data pipeline. optional]
        [ -u | Undeploy]"
    exit 1
}

undeploy=false
build_semi_automated_data_pipeline=false

while getopts ":r:t:a:p:v:s:d:bu" opt; do
    case $opt in
        r ) region=$OPTARG ;;
        t ) pce_id=$OPTARG ;;
        a ) aws_account_id=$OPTARG ;;
        p ) publisher_aws_account_id=$OPTARG ;;
        v ) publisher_vpc_id=$OPTARG ;;
        s ) s3_bucket_for_storage=$OPTARG ;;
        d ) s3_bucket_data_pipeline=$OPTARG ;;
        b ) build_semi_automated_data_pipeline=true ;;
        u ) undeploy=true ;;
        * ) usage ;;
    esac
done

tag_postfix="-${pce_id}"

if [ -z ${TF_LOG+x} ]; then
    echo "Terraform Detailed Error Logging Disabled"
else
    echo "Terraform Log Level: $TF_LOG"
    echo "Terraform Log File: $TF_LOG_PATH"
    echo
fi

echo "AWS region is $region."
echo "The string '$tag_postfix' will be appended after the tag of the AWS resources."
echo "Your AWS acount ID is $aws_account_id"
echo "Publisher's AWS account ID is $publisher_aws_account_id"
echo "Publisher's VPC ID is $publisher_vpc_id"
echo "The S3 bucket for storing 1) Terraform state file, 2) AWS Lambda functions, and 3) config.yml is $s3_bucket_for_storage"
echo "The S3 bucket for storing processed data is $s3_bucket_data_pipeline, will be created in a short while...".

##########################################
# Helper functions
##########################################
check_s3_object_exist() {
    local bucket_name=$1
    local key_name=$2
    local account_id=$3
    aws s3api head-object --bucket "$bucket_name" --key "$key_name" --expected-bucket-owner "$account_id" || not_exist=true
    if [ $not_exist ]; then
        echo "The tfstate file $key_name does not exist. Exiting..."
        false
    else
        echo "The tfstate file $key_name exists."
        true
    fi
}

undeploy_aws_resources () {
    echo "Start undeploying AWS resource under PCE_shared..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/pce_shared$tag_postfix.tfstate" "$aws_account_id"
    echo "Related tfstate file exists. Continue..."
    echo "########################Deleting########################"

    cd /terraform_deployment/terraform_scripts/common/pce_shared
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pce_shared$tag_postfix.tfstate"

    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "pce_id=$pce_id"
    echo "Finished undeploying AWS resource under PCE_shared."
    echo "Start undeploying AWS resource under PCE..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/pce$tag_postfix.tfstate" "$aws_account_id"
    echo "Related tfstate file exists. Continue..."
    echo "########################Deleting########################"
    cd /terraform_deployment/terraform_scripts/common/pce
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/pce$tag_postfix.tfstate"

    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "pce_id=$pce_id"
    echo "Finished undeploying AWS resource under PCE."
    echo "Start undeploying AWS resource under VPC peering..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/vpcpeering$tag_postfix.tfstate" "$aws_account_id"
    echo "All tfstate files exist. Continue..."
    echo "########################Deleting########################"
    cd /terraform_deployment/terraform_scripts/partner/vpc_peering
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/vpcpeering$tag_postfix.tfstate"

    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "pce_id=$pce_id"

    echo "Finished undeploying AWS resource under VPC peering."
    echo "Start undeploying AWS resource under Data Ingestion..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/data_ingestion$tag_postfix.tfstate" "$aws_account_id"
    echo "All tfstate files exist. Continue..."
    echo "########################Deleting########################"

    cd /terraform_deployment/terraform_scripts/data_ingestion

    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/data_ingestion$tag_postfix.tfstate"

    # Exclude the s3 bucket because it can not be deleted if it's not empty
    terraform state rm aws_s3_bucket.bucket || true

    terraform destroy \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id"

    if "$build_semi_automated_data_pipeline"
    then
        echo "Undeploy Semi automated data_pipeline..."
        check_s3_object_exist "$s3_bucket_for_storage" "tfstate/glue_etl$tag_postfix.tfstate" "$aws_account_id"
        echo "Semi automated data_pipeline tfstate file exists. Continue..."
        cd /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
        terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/glue_etl$tag_postfix.tfstate"

        # Exclude the s3 bucket because it can not be deleted if it's not empty
        terraform state rm aws_s3_bucket.bucket || true
        terraform destroy \
            -auto-approve \
            -var "region=$region" \
            -var "tag_postfix=$tag_postfix" \
            -var "aws_account_id=$aws_account_id"
    fi

    echo "Finished destroy all AWS resources, except for S3 buckets (can not be deleted if it's not empty)"

}


input_validation () {
echo "######################input validation############################"
echo "validate input: aws account id..."
account_A=$(aws sts get-caller-identity |grep -o 'Account":.*' | tr -d '"' | tr -d ' ' | tr -d ',' | cut -d':' -f2)
account_B=$aws_account_id
if [ "$account_A" == "$account_B" ]
then
    echo "input AWS account is valid."
else # not equal
    echo "Error: the provided AWS account id does not match the configured [secret_key, access_key]"
    exit 1
fi
}
##########################################
# Main
##########################################

# validate whether the input aws_account_id matches the account from the configured [secret_key, access_key] pair
input_validation


if "$undeploy"
then
    echo "Undeploying the AWS resources..."
    undeploy_aws_resources
    exit 0
fi

# Create the S3 bucket if it doesn't exist
echo "######################## Create S3 buckets if don't exist ########################"
if aws s3api head-bucket --bucket "$s3_bucket_for_storage" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "404" # bucekt doesn't exist
then
    echo "The bucket $s3_bucket_for_storage doesn't exist. Creating..."
    aws s3api create-bucket --bucket "$s3_bucket_for_storage" --region "$region" --create-bucket-configuration LocationConstraint="$region" || exit 1
    aws s3api put-bucket-versioning --bucket "$s3_bucket_for_storage" --versioning-configuration Status=Enabled
    echo "The bucket $s3_bucket_for_storage is created."
elif aws s3api head-bucket --bucket "$s3_bucket_for_storage" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "403" # no access to the bucket
then
    echo "the bucket $s3_bucket_for_storage is owned by a different account."
    echo "Please check your whether your AWS account id $aws_account_id matches your secret key and access key provided"
    exit 1
else
    echo "The bucket $s3_bucket_for_storage exists and you have access to it. Using it for storing Terraform state..."
fi


# Deploy PCE Terraform scripts
onedocker_ecs_container_image='539290649537.dkr.ecr.us-west-2.amazonaws.com/one-docker-prod:latest'
publisher_vpc_cidr='10.0.0.0/16'

echo "########################Deploy PCE Terraform scripts########################"
cd /terraform_deployment/terraform_scripts/common/pce_shared
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/pce_shared$tag_postfix.tfstate"
terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "aws_account_id=$aws_account_id" \
    -var "onedocker_ecs_container_image=$onedocker_ecs_container_image" \
    -var "pce_id=$pce_id"

# Store the outputs into variables
onedocker_task_definition_family=$(terraform output onedocker_task_definition_family | tr -d '"')
onedocker_task_definition_revision=$(terraform output onedocker_task_definition_revision | tr -d '"')
onedocker_task_definition_container_definiton_name=$(terraform output onedocker_task_definition_container_definitons | jq 'fromjson | .[].name' | tr -d '"')

cd /terraform_deployment/terraform_scripts/common/pce
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/pce$tag_postfix.tfstate"
terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "otherparty_vpc_cidr=$publisher_vpc_cidr" \
    -var "pce_id=$pce_id"

# Store the outputs into variables
vpc_id=$(terraform output vpc_id | tr -d '"' )
subnet_ids=$(terraform output subnets | tr -d '""[]\ \n')
route_table_id=$(terraform output route_table_id | tr -d '"')
aws_ecs_cluster_name=$(terraform output aws_ecs_cluster_name | tr -d '"')

# Issue VPC Peering Connection to Publisher's VPC and add a route to the route table
echo "########################Issue VPC Peering connection to Publisher's VPC########################"
cd /terraform_deployment/terraform_scripts/partner/vpc_peering
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/vpcpeering$tag_postfix.tfstate"
terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "peer_aws_account_id=$publisher_aws_account_id" \
    -var "peer_vpc_id=$publisher_vpc_id" \
    -var "vpc_id=$vpc_id" \
    -var "route_table_id=$route_table_id" \
    -var "destination_cidr_block=$publisher_vpc_cidr" \
    -var "pce_id=$pce_id"

# Store the outputs into variables
vpc_peering_connection_id=$(terraform output vpc_peering_connection_id | tr -d '"' )
echo "VPC peering connection has been created. ID: $vpc_peering_connection_id"

# Configure Data Ingestion Pipeline from CB to S3
echo "########################Configure Data Ingestion Pipeline from CB to S3########################"
cd /terraform_deployment/terraform_scripts/data_ingestion
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/data_ingestion$tag_postfix.tfstate"

terraform apply \
    -auto-approve \
    -var "region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "aws_account_id=$aws_account_id" \
    -var "data_processing_output_bucket=$s3_bucket_data_pipeline" \
    -var "data_processing_lambda_s3_bucket=$s3_bucket_for_storage" \
    -var "data_processing_lambda_s3_key=lambda.zip"
# store the outputs from data ingestion pipeline output into variables
app_data_input_bucket_id=$(terraform output data_processing_output_bucket_id | tr -d '"')
app_data_input_bucket_arn=$(terraform output data_processing_output_bucket_arn | tr -d '"')

if "$build_semi_automated_data_pipeline"
then
    echo "########################Configure Semi-automated Data Ingestion Pipeline from CB to S3########################"

    # configure semi-automated data ingestion pipeline, if true
    cd /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
    echo "Updating trigger function configurations..."
    sed -i "s/glueJobName = 'TO_BE_UPDATED_DURING_DEPLOYMENT'/glueJobName = 'glue-ETL$tag_postfix'/g" lambda_trigger.py
    sed -i "s~s3_write_path = 'TO_BE_UPDATED_DURING_DEPLOYMENT'~s3_write_path = '$app_data_input_bucket_id'~g" lambda_trigger.py

    echo "Running terraform installation..."
    terraform init \
        -backend-config "bucket=$s3_bucket_for_storage" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/glue_etl$tag_postfix.tfstate"

    terraform apply \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "lambda_trigger_s3_key=lambda_trigger.zip" \
        -var "app_data_input_bucket=$s3_bucket_data_pipeline" \
        -var "app_data_input_bucket_id=$app_data_input_bucket_id" \
        -var "app_data_input_bucket_arn=$app_data_input_bucket_arn"

    exit 0
fi

echo "########################Finished AWS Infrastructure Deployment########################"

echo "########################Start populating config.yml ########################"
cd /terraform_deployment
sed -i "s/region: .*/region: $region/g" config.yml
echo "Populated region with value $region"

sed -i "s/cluster: .*/cluster: $aws_ecs_cluster_name/g" config.yml
echo "Populated cluster with value $aws_ecs_cluster_name"

sed -i "s/subnets: .*/subnets: [${subnet_ids}]/g" config.yml
echo "Populated subnets with value '[${subnet_ids}]'"

onedocker_task_definition=$onedocker_task_definition_family:$onedocker_task_definition_revision#$onedocker_task_definition_container_definiton_name
sed -i "s/task_definition: TODO_ONEDOCKER_TASK_DEFINITION/task_definition: $onedocker_task_definition/g" config.yml
echo "Populated Onedocker - task_definition with value $onedocker_task_definition"

sed -i "/access_key_id/d" config.yml
sed -i "/access_key_data/d" config.yml
echo "Removed the credential lines"

echo "########################Upload config.ymls to S3########################"
cd /terraform_deployment
aws s3api put-object --bucket "$s3_bucket_for_storage" --key "config.yml" --body ./config.yml
