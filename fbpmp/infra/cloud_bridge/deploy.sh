#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

usage() {
    echo "Usage: deploy.sh
        [ -r | AWS region, e.g. us-west-2 ]
        [ -t | Tag that going to be appended after the resource name]
        [ -a | Your AWS account ID]
        [ -p | Publisher's AWS account ID]
        [ -v | Publisher's VPC Id]
        [ -s | S3 bucket name for storing tfstate/lambda function]
        [ -d | S3 bucket name for storing lambda processed results]
        [ -u | Undeploy]"
    exit 1
}

undeploy=false

while getopts ":r:t:a:p:v:s:d:u" opt; do
    case $opt in
        r ) region=$OPTARG ;;
        t ) tag_postfix=$OPTARG ;;
        a ) aws_account_id=$OPTARG ;;
        p ) publisher_aws_account_id=$OPTARG ;;
        v ) publisher_vpc_id=$OPTARG ;;
        s ) s3_bucket_for_storage=$OPTARG ;;
        d ) s3_bucket_data_pipeline=$OPTARG ;;
        u ) undeploy=true ;;
        * ) usage ;;
    esac
done

echo "AWS region is $region."
echo "The string '$tag_postfix' will be appended after the tag of the AWS resources."
echo "Your AWS acount ID is $aws_account_id"
echo "Publisher's AWS account ID is $publisher_aws_account_id"
echo "Publisher's VPC ID is $publisher_vpc_id"
echo "The S3 bucket for storing the Terraform state file and/or lambda function is $s3_bucket_for_storage"
echo "The S3 bucket for storing lambda processed events is $s3_bucket_data_pipeline, will be created in a short while...".

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
    echo "Start undeploying..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/vpcpeering$tag_postfix.tfstate" "$aws_account_id"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/ecs$tag_postfix.tfstate" "$aws_account_id"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/networking$tag_postfix.tfstate" "$aws_account_id"
    check_s3_object_exist "$s3_bucket_for_storage" "tfstate/data_ingestion$tag_postfix.tfstate" "$aws_account_id"
    echo "All tfstate files exist. Continue..."

    echo "########################Delete VPC Peering connection########################"
    cd /terraform_deployment/terraform_scripts/vpc_peering
    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \

    echo "########################Delete ECS related resources########################"
    cd /terraform_deployment/terraform_scripts/ecs
    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id"

    echo "########################Delete networking related resources########################"
    cd /terraform_deployment/terraform_scripts/networking
    terraform destroy \
        -auto-approve \
        -var "aws_region=$region" \
        -var "tag_postfix=$tag_postfix"

    echo "########################Delete Data Ingestion related resources########################"
    cd /terraform_deployment/terraform_scripts/data_ingestion
    # Exclude the s3 bucket because it can not be delted if it's not empty
    terraform state rm aws_s3_bucket.bucket || true
    terraform destroy \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id"
}

##########################################
# Main
##########################################

if "$undeploy"
then
    echo "Undeploying the AWS resources..."
    undeploy_aws_resources
    exit 0
fi

# Create the S3 bucket if it doesn't exist
echo "########################Create S3 buckets if don't exist ########################"
if aws s3api head-bucket --bucket "$s3_bucket_for_storage" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "404" # bucekt doesn't exist
then
    echo "The bucket $s3_bucket_for_storage doesn't exist. Creating..."
    aws s3api create-bucket --bucket "$s3_bucket_for_storage" --region "$region" --create-bucket-configuration LocationConstraint="$region" || exit 1
    aws s3api put-bucket-versioning --bucket "$s3_bucket_for_storage" --versioning-configuration Status=Enabled
    echo "The bucket $s3_bucket_for_storage is created."
elif aws s3api head-bucket --bucket "$s3_bucket_for_storage" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "403" # no access to the bucket
then
    echo "You don't have access to the bucket $s3_bucket_for_storage. Please choose another name."
    exit 1
else
    echo "The bucket $s3_bucket_for_storage exists and you have access to it. Using it for storing Terraform state..."
fi


# Install networking realted Terraform scripts
echo "########################Deploy networking related Terraform scripts########################"
cd /terraform_deployment/terraform_scripts/networking
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/networking$tag_postfix.tfstate"
terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "publisher_vpc_cidrs=[\"10.0.0.0/16\"]"

# Store the outputs into variables
vpc_id=$(terraform output vpc_id | tr -d '"' )
subnet0_id=$(terraform output subnet0_id | tr -d '"' )
subnet1_id=$(terraform output subnet1_id | tr -d '"')
pl_efs_security_group_id=$(terraform output pl_efs_security_group_id | tr -d '"')
route_table_id=$(terraform output route_table_id | tr -d '"')

# Install ecs realted Terraform scripts
echo "########################Deploy ECS related Terraform scripts########################"
cd /terraform_deployment/terraform_scripts/ecs
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/ecs$tag_postfix.tfstate"
terraform apply \
    -auto-approve \
    -var "aws_region=$region" \
    -var "tag_postfix=$tag_postfix" \
    -var "aws_account_id=$aws_account_id" \
    -var "subnet0_id=$subnet0_id" \
    -var "subnet1_id=$subnet1_id" \
    -var "pl_efs_security_group_id=$pl_efs_security_group_id" \
    -var 'data_processing_ecs_container_image=539290649537.dkr.ecr.us-west-2.amazonaws.com/data-processing:latest' \
    -var 'pid_ecs_container_image=539290649537.dkr.ecr.us-west-2.amazonaws.com/private-id:docker-built' \
    -var 'pl_ecs_container_image=539290649537.dkr.ecr.us-west-2.amazonaws.com/one-docker-prod:latest'

# Store the outputs into variables
aws_ecs_cluster_name=$(terraform output aws_ecs_cluster_name | tr -d '"')
pid_task_definition_family=$(terraform output pid_task_definition_family | tr -d '"')
pid_task_definition_revision=$(terraform output pid_task_definition_revision | tr -d '"')
pid_task_definition_container_definiton_name=$(terraform output pid_task_definition_container_definitons | jq 'fromjson | .[].name' | tr -d '"')
pl_task_definition_family=$(terraform output pl_task_definition_family | tr -d '"')
pl_task_definition_revision=$(terraform output pl_task_definition_revision | tr -d '"')
pl_task_definition_container_definiton_name=$(terraform output pl_task_definition_container_definitons | jq 'fromjson | .[].name' | tr -d '"')
data_processing_task_definition_family=$(terraform output data_processing_task_definition_family | tr -d '"')
data_processing_task_definition_revision=$(terraform output data_processing_task_definition_revision | tr -d '"')
data_processing_task_definition_container_definiton_name=$(terraform output data_processing_task_definition_container_definitons | jq 'fromjson | .[].name' | tr -d '"')

# Issue VPC Peering Connection to Publisher's VPC
echo "########################Issue VPC Peering connection to Publisher's VPC########################"
cd /terraform_deployment/terraform_scripts/vpc_peering
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
    -var "vpc_id=$vpc_id"

# Store the outputs into variables
vpc_peering_connection_id=$(terraform output vpc_peering_connection_id | tr -d '"' )

# Add one route to the route table to route the traffic to the peering connection
echo "########################Add one route to the route table########################"
cd /terraform_deployment/terraform_scripts/traffic_route
terraform init \
    -backend-config "bucket=$s3_bucket_for_storage" \
    -backend-config "region=$region" \
    -backend-config "key=tfstate/trafficroute$tag_postfix.tfstate"

terraform apply \
    -auto-approve \
    -var "route_table_id=$route_table_id" \
    -var "destination_cidr_block=10.0.0.0/16" \
    -var "vpc_peering_connection_id=$vpc_peering_connection_id"

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


echo "########################Finished AWS Infrastructure Deployment########################"

echo "########################Start populating config.yml########################"
cd /terraform_deployment
sed -i "s/region: .*/region: $region/g" config.yml
echo "Populated region with value $region"

sed -i "s/cluster: .*/cluster: $aws_ecs_cluster_name/g" config.yml
echo "Populated cluster with value $aws_ecs_cluster_name"

sed -i "s/subnets: .*/subnets: [$subnet0_id]/g" config.yml
echo "Populated subnets with value '[$subnet0_id]'"

pid_task_definition=$pid_task_definition_family:$pid_task_definition_revision#$pid_task_definition_container_definiton_name
sed -i "s/task_definition: \(TODO_PID\|pid-.*\)/task_definition: $pid_task_definition/g" config.yml
echo "Populated PID - task_definition with value $pid_task_definition"

pl_task_definition=$pl_task_definition_family:$pl_task_definition_revision#$pl_task_definition_container_definiton_name
sed -i "s/task_definition: \(TODO_MPC\|pl-.*\)/task_definition: $pl_task_definition/g" config.yml
echo "Populated MPC - task_definition with value $pl_task_definition"

sed -i "/access_key_id/d" config.yml
sed -i "/access_key_data/d" config.yml
echo "Removed the credential lines"

echo "########################Upload config.yml to S3########################"
cd /terraform_deployment
aws s3api put-object --bucket "$s3_bucket_for_storage" --key "config.yml" --body ./config.yml
