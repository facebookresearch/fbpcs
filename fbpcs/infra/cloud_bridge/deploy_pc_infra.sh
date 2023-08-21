#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e
# shellcheck disable=SC1091
# shellcheck disable=SC1090

source "$(dirname "$(realpath "$0")")/util.sh"
usage() {
    echo "Usage: deploy_pc_infra.sh <deploy|undeploy>
        [ -r, --region | AWS region, e.g. us-west-2 ]
        [ -t, --tag | Tag that going to use as the env id and appended after the resource name]
        [ -a, --account_id | Your AWS account ID]
        [ -b, --build_semi_automated_data_pipeline | optional. whether to build semi automated (manual upload) data pipeline ]"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

undeploy=false
build_semi_automated_data_pipeline=false

case "$1" in
    deploy) ;;
    undeploy) undeploy=true ;;
    *) usage ;;
esac
shift

while [ $# -gt 0 ]; do
    second_shift_flag=true
    case "$1" in
        -r|--region) region="$2" ;;
        -t|--tag) env_tag="$2" ;;
        -a|--account_id) aws_account_id="$2" ;;
        -b|--build_semi_automated_data_pipeline) build_semi_automated_data_pipeline=true second_shift_flag=false ;;
        *) usage ;;
    esac
    shift
    test "$second_shift_flag" == "true" && shift
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


undeploy_aws_resources() {
    # validate all the inputs
    log_streaming_data "starting to undeploy resources"
    input_validation "$region" "$env_tag" "$aws_account_id" "" "" "$s3_bucket_config" "$s3_bucket_data" "$build_semi_automated_data_pipeline" "$undeploy"

    echo "Start undeploying AWS resource under Data Ingestion..."
    echo "########################Check tfstate files########################"
    check_s3_object_exist "$s3_bucket_config" "tfstate/data_ingestion$tag_postfix.tfstate" "$aws_account_id"
    echo "Related tfstate files exists. Continue..."
    echo "########################Deleting########################"
    log_streaming_data "starting to undeploy data ingestion resources "
    cd /terraform_deployment/terraform_scripts/data_ingestion
    echo "######################## Initializing terraform working directory before deleting resources ########################"
    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/data_ingestion$tag_postfix.tfstate"
    echo "######################## Initializing terraform working directory completed ########################"
    # Exclude the s3 bucket because it can not be deleted if it's not empty
    terraform state rm aws_s3_bucket.bucket || true
    echo "########################Deleting########################"
    echo "########################Ensuring Glue job $glue_crawler_name is stopped########################"
    stopGlueCrawlerJob "$glue_crawler_name" "$region"
    terraform destroy \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "data_processing_lambda_s3_bucket=$s3_bucket_config" \
        -var "data_processing_lambda_s3_key=lambda.zip" \
        -var "data_upload_key_path=$data_upload_key_path" \
        -var "query_results_key_path=$query_results_key_path"
    echo "########################Deletion completed########################"

    echo "######################## Delete KIA Lambda fuction ########################"
    cd /terraform_deployment/terraform_scripts/key_injection_agent/

    log_streaming_data "starting to undeploy key injection agent."

    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/key_injection_agent_$tag_postfix.tfstate"

    terraform destroy \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "kia_lambda_function_name=$kia_lambda_function_name" \
        -var "kia_lambda_input_bucket=$s3_bucket_data" \
        -var "kia_lambda_s3_bucket=$s3_bucket_config" \
        -var "kia_lambda_s3_key=kialambda.zip"

    log_streaming_data "undeployed key injection agent."

    echo "######################## Deleted KIA Lambda fuction ########################"

    echo "######################## Delete Pl Clean Up Agent Lambda fuction ########################"
    cd /terraform_deployment/terraform_scripts/clean_up_agent/

    log_streaming_data "starting to undeploy Pl Clean up agent."

    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/clean_up_agent_$tag_postfix.tfstate"

    terraform destroy \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "clean_up_agent_lambda_function_name=$clean_up_agent_lambda_function_name" \
        -var "clean_up_agent_lambda_input_bucket=$s3_bucket_data" \
        -var "clean_up_agent_lambda_source_bucket=$s3_bucket_config" \
        -var "clean_up_agent_lambda_s3_key=source.zip"

    log_streaming_data "undeployed Pl Clean up agent."

    echo "######################## Deleted Pl Clean Up Agent Lambda fuction ########################"

    if "$build_semi_automated_data_pipeline"
    then
        echo "Undeploy Semi automated data_pipeline..."
        log_streaming_data "starting to undeploy data_pipeline "
        check_s3_object_exist "$s3_bucket_config" "tfstate/glue_etl$tag_postfix.tfstate" "$aws_account_id"
        echo "Semi automated data_pipeline tfstate file exists. Continue..."
        cd /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
        # lambda_trigger.py needs to be copied here in case a deploy was not previously run in the container
        cp template/lambda_trigger.py .
        terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/glue_etl$tag_postfix.tfstate"

        # Exclude the s3 bucket because it can not be deleted if it's not empty
        terraform state rm aws_s3_bucket.bucket || true
        terraform destroy \
            -auto-approve \
            -var "region=$region" \
            -var "tag_postfix=$tag_postfix" \
            -var "aws_account_id=$aws_account_id" \
            -var "data_upload_key_path=$data_upload_key_path"
    fi
    echo "######################## Undeploy resources policy ########################"
    log_streaming_data "Undeploying resources policies..."
    echo "Deleting policy: $policy_name"
    cd /terraform_deployment
    python3 cli.py destroy aws \
        --delete_iam_policy \
        --policy_name "$policy_name"

    echo "Deleting data bucket policy: $data_bucket_policy_name"
    python3 cli.py destroy aws \
        --delete_iam_policy \
        --policy_name "$data_bucket_policy_name"
    echo "######################## Finished undeploy resources policy ########################"

    log_streaming_data "finished undeploying all AWS resources "
    echo "Finished destroying all AWS resources, except for:"
    echo "  # S3 storage bucket ${s3_bucket_config}"
    echo "  # S3 data bucket ${s3_bucket_data}"
    echo "The following resources may have been deleted:"
    echo "  # IAM policy ${policy_name} (it will be deleted only if it is not attached to any users)"
    log_streaming_data "undeployment process finished"
}


deploy_aws_resources() {
    # first log, making sure the file is re-written fresh
    echo "{}" > "$TF_RESOURCE_OUTPUT"
    log_streaming_data "starting to deploy resources..."
    log_streaming_data "validating inputs..."
    # validate all the inputs
    input_validation "$region" "$env_tag" "$aws_account_id" "" "" "$s3_bucket_config" "$s3_bucket_data" "$build_semi_automated_data_pipeline" "$undeploy"
    #clean up previously generated resources if any
    cleanup_generated_resources

    # Create the S3 bucket (to store config files) if it doesn't exist
    log_streaming_data "creating s3 config bucket, if it does not exist"
    validate_or_create_s3_bucket "$s3_bucket_config" "$region" "$aws_account_id"
    # Create the S3 data bucket if it doesn't exist
    log_streaming_data "creating s3 data bucket, if it does not exist"
    validate_or_create_s3_bucket "$s3_bucket_data" "$region" "$aws_account_id"

    # Create data bucket policy
    echo "########################Create data bucket policy########################"
    cd /terraform_deployment
    python3 cli.py create aws \
        --add_iam_policy \
        --policy_name "$data_bucket_policy_name" \
        --template_path "$fb_pc_data_bucket_policy" \
        --region "$region" \
        --data_bucket_name "$s3_bucket_data"
    echo "########################Done creating data bucket policy########################"


    # Configure Data Ingestion Pipeline from CB to S3
    echo "########################Configure Data Ingestion Pipeline from CB to S3########################"
    cd /terraform_deployment/terraform_scripts/data_ingestion
    echo "######################## Initializing terraform working directory started ########################"
    log_streaming_data "configuring data ingestion pipeline..."
    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/data_ingestion$tag_postfix.tfstate"
    echo "######################## Initializing terraform working directory completed ########################"
    echo "######################## Deploy Data Ingestion Terraform scripts started ########################"
    set +e
    local data_ingestion_time_out=600
    SECONDS=0
    while [ $SECONDS -lt $data_ingestion_time_out ]
    do
        terraform apply \
            -auto-approve \
            -var "region=$region" \
            -var "tag_postfix=$tag_postfix" \
            -var "aws_account_id=$aws_account_id" \
            -var "data_processing_output_bucket=$s3_bucket_data" \
            -var "data_processing_output_bucket_arn=$data_bucket_arn" \
            -var "data_ingestion_lambda_name=$data_ingestion_lambda_name" \
            -var "data_processing_lambda_s3_bucket=$s3_bucket_config" \
            -var "data_processing_lambda_s3_key=lambda.zip" \
            -var "data_upload_key_path=$data_upload_key_path" \
            -var "query_results_key_path=$query_results_key_path"
        local return_code=$?
        echo "Checking if the Data Ingestion deployment was successful..."
        if [[ $return_code -eq 0 ]]; then
            echo "Successfully created the Data Ingestion infra"
            break
        fi
        if [[ $return_code -ne 0 ]] && [[ $SECONDS -gt $data_ingestion_time_out ]]; then
            echo "Error: creating the Data Ingestion infra timed out"
            set -e
            exit 1
        fi
        echo "Warning - Data Ingestion provisioning failed. Retrying..."
        sleep 5
    done
    set -e
    echo "######################## Deploy Data Ingestion Terraform scripts completed ########################"
    # store the outputs from data ingestion pipeline output into variables
    firehose_stream_name=$(terraform output firehose_stream_name | tr -d '"')
    events_data_crawler_arn=$(terraform output events_data_crawler_arn | tr -d '"')

    if "$build_semi_automated_data_pipeline"
    then
        echo "########################Configure Semi-automated Data Ingestion Pipeline from CB to S3########################"
        log_streaming_data "configuring semi-automated data ingestion pipeline from CAPI-G to s3"
        # configure semi-automated data ingestion pipeline, if true
        cd /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
        # copy the lambda_trigger.py template to the local directory
        cp template/lambda_trigger.py .
        echo "Updating trigger function configurations..."
        sed -i "s/glueJobName = \"TO_BE_UPDATED_DURING_DEPLOYMENT\"/glueJobName = \"glue-ETL$tag_postfix\"/g" lambda_trigger.py
        sed -i "s~s3_write_path = \"TO_BE_UPDATED_DURING_DEPLOYMENT\"~s3_write_path = \"$s3_bucket_data/events_data/\"~g" lambda_trigger.py

        echo "######################## Initializing terraform working directory started ########################"
        terraform init -reconfigure \
            -backend-config "bucket=$s3_bucket_config" \
            -backend-config "region=$region" \
            -backend-config "key=tfstate/glue_etl$tag_postfix.tfstate"
        echo "######################## Initializing terraform working directory completed ########################"
        echo "######################## Deploy Semi-automated Data Ingestion Terraform scripts started ########################"
        terraform apply \
            -auto-approve \
            -var "region=$region" \
            -var "tag_postfix=$tag_postfix" \
            -var "aws_account_id=$aws_account_id" \
            -var "lambda_trigger_s3_key=lambda_trigger.zip" \
            -var "app_data_input_bucket=$s3_bucket_data" \
            -var "app_data_input_bucket_id=$s3_bucket_data" \
            -var "app_data_input_bucket_arn=$data_bucket_arn" \
            -var "data_upload_key_path=$data_upload_key_path"
        echo "######################## Deploy Semi-automated Data Ingestion Terraform scripts completed ########################"
        # Store the outputs into variables
        semi_automated_glue_job_arn=$(terraform output semi_automated_glue_job_arn | tr -d '"')
    fi

    echo "######################## Deploying Clean Up Agent Agent AWS Lambda"
    cd /terraform_deployment/terraform_scripts/clean_up_agent

    log_streaming_data "starting to deploy Clean Up agent."

    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/clean_up_agent_$tag_postfix.tfstate"

    terraform apply \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "clean_up_agent_lambda_function_name=$clean_up_agent_lambda_function_name" \
        -var "clean_up_agent_lambda_input_bucket=$s3_bucket_data" \
        -var "clean_up_agent_lambda_source_bucket=$s3_bucket_config" \
        -var "clean_up_agent_lambda_s3_key=source.zip"

    log_streaming_data "deployed clean up agent."

    echo "######################## Deployed Clean Up Agent AWS Lambda"
    # Store the clean up agent IAM role arn as a variable, we need to send this to KIA lambda.
    clean_up_agent_lambda_iam_role_arn=$(terraform output clean_up_agent_lambda_iam_role_arn | tr -d '"')

    echo "######################## Deploying Key Injection Agent AWS Lambda"
    cd /terraform_deployment/terraform_scripts/key_injection_agent

    log_streaming_data "starting to deploy key injection agent."

    terraform init -reconfigure \
        -backend-config "bucket=$s3_bucket_config" \
        -backend-config "region=$region" \
        -backend-config "key=tfstate/key_injection_agent_$tag_postfix.tfstate"

    terraform apply \
        -auto-approve \
        -var "region=$region" \
        -var "tag_postfix=$tag_postfix" \
        -var "aws_account_id=$aws_account_id" \
        -var "kia_lambda_function_name=$kia_lambda_function_name" \
        -var "kia_lambda_input_bucket=$s3_bucket_data" \
        -var "kia_lambda_s3_bucket=$s3_bucket_config" \
        -var "clean_up_agent_lambda_iam_role=$clean_up_agent_lambda_iam_role_arn" \
        -var "kia_lambda_s3_key=kialambda.zip"

    log_streaming_data "deployed key injection agent."

    echo "######################## Deployed Key Injection Agent AWS Lambda"

    echo "########################Finished AWS Infrastructure Deployment########################"
    log_streaming_data "finished deploying resources..."

    echo "######################## Deploy resources policy ########################"
    log_streaming_data "deploying resources policies..."
    cd /terraform_deployment

    python3 cli.py create aws \
        --add_iam_policy \
        --policy_name "$policy_name" \
        --template_path "$fb_pc_iam_policy" \
        --region "$region" \
        --firehose_stream_name "$firehose_stream_name" \
        --data_ingestion_lambda_name "$data_ingestion_lambda_name" \
        --kia_lambda_name "$kia_lambda_function_name" \
        --data_bucket_name "$s3_bucket_data" \
        --config_bucket_name "$s3_bucket_config" \
        --database_name "$database_name" \
        --table_name "$table_name" \
        --events_data_crawler_arn "$events_data_crawler_arn" \
        --semi_automated_glue_job_arn "$semi_automated_glue_job_arn"
    echo "######################## Finished deploy resources policy ########################"
}


##########################################
# Main
##########################################
tag_postfix="-${env_tag}"

s3_bucket_config="fb-pc-config$tag_postfix"
s3_bucket_data="fb-pc-data$tag_postfix"
data_bucket_arn="arn:aws:s3:::${s3_bucket_data}"
policy_name="fb-pc-policy${tag_postfix}"
database_name="mpc-events-db${tag_postfix}"
glue_crawler_name="mpc-events-crawler${tag_postfix}"
table_name=${s3_bucket_data//-/_}
data_upload_key_path="semi-automated-data-ingestion"
query_results_key_path="query-results"
data_ingestion_lambda_name="cb-data-ingestion-stream-processor${tag_postfix}"
kia_lambda_function_name="cb-kia${tag_postfix}"
clean_up_agent_lambda_function_name="cb-clean-up-agent${tag_postfix}"
fb_pc_iam_policy="/terraform_deployment/fbpcs/infra/cloud_bridge/deployment_helper/aws/iam_policies/fb_pc_iam_policy_no_compute.json"
fb_pc_data_bucket_policy="/terraform_deployment/fbpcs/infra/cloud_bridge/deployment_helper/aws/iam_policies/fb_pc_data_bucket_policy.json"
data_bucket_policy_name="fb-pc-data-bucket-policy${tag_postfix}"

if "$undeploy"
then
    echo "Undeploying the AWS resources..."
    undeploy_aws_resources
else
    echo "Deploying AWS resources..."
    deploy_aws_resources
fi
exit 0
