#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

real_dir_path=$(dirname "$(realpath "$0")")

# shellcheck disable=SC1091
# shellcheck disable=SC1090
source "$real_dir_path"/util.sh
usage() {
    echo "This is the script to upgrade advertiser-side infra (MPC -> TEE). Usage: upgrade.sh
        [ -r, --region | AWS region, e.g. us-west-2 ]
        [ -t, --tag | Tag that going to use as the env id and appended after the resource name;  previously used as the PCE id for MPC deployment]
        [ -a, --account_id | Your AWS account ID]
        [ -p, --publisher_account_id | Publisher's AWS account ID]
        [ -v, --publisher_vpc_id | Publisher's VPC Id]
        [ -s, --config_storage_bucket | optional. S3 bucket name for storing configs: tfstate/lambda function]
        [ -d, --data_storage_bucket | optional. S3 bucket name for storing lambda processed results]
        [ -b, --build_semi_automated_data_pipeline | optional. whether to build semi automated (manual upload) data pipeline ]"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

build_semi_automated_data_pipeline=false

while [ $# -gt 0 ]; do
    second_shift_flag=true
    case "$1" in
        -r|--region) region="$2" ;;
        -t|--tag) env_tag="$2" tee_env_tag="$2-tee";;
        -a|--account_id) aws_account_id="$2" ;;
        -p|--publisher_account_id) publisher_aws_account_id="$2" ;;
        -v|--publisher_vpc_id) publisher_vpc_id="$2" ;;
        -s|--config_storage_bucket) s3_bucket_for_storage="$2" ;;
        -d|--data_storage_bucket) s3_bucket_data_pipeline="$2" ;;
        -b|--build_semi_automated_data_pipeline) build_semi_automated_data_pipeline=true second_shift_flag=false ;;
        *) usage ;;
    esac
    shift
    test "$second_shift_flag" == "true" && shift
done


function undeploy_old_infra() {
    # Wrapping undeploy old infra command
    undeploy_old_cmd=("$real_dir_path"/deploy.sh undeploy -r "$region" -t "$env_tag" -a "$aws_account_id" -p "$publisher_aws_account_id" -v "$publisher_vpc_id")
    if [ -n "$s3_bucket_for_storage" ]
    then
        # s3_bucket_data_pipeline is set
        undeploy_old_cmd+=(-s "$s3_bucket_for_storage")
    fi

    if [ -n "$s3_bucket_data_pipeline" ]
    then
        # s3_bucket_data_pipeline is set
        undeploy_old_cmd+=(-d "$s3_bucket_data_pipeline")
    fi

    if [ "$build_semi_automated_data_pipeline" = true ];
    then
        undeploy_old_cmd+=("-b")
    fi

    echo "===== Executing undeploy old infra =====:" "${undeploy_old_cmd[@]}"
    if ! "${undeploy_old_cmd[@]}";
    then
        echo "Error occurred when executing undeploy old infra, please retry again. Exiting..."
        exit 65
    fi
}

function deploy_pc_infra() {
    # Wrapping deploy pc infra command
    deploy_pc_infra_cmd=("$real_dir_path"/deploy_pc_infra.sh deploy -r "$region" -t "$tee_env_tag" -a "$aws_account_id")
    if [ "$build_semi_automated_data_pipeline" = true ];
    then
        deploy_pc_infra_cmd+=("-b")
    fi

    echo "===== Executing deploy pc infra =====:" "${deploy_pc_infra_cmd[@]}"
    if ! "${deploy_pc_infra_cmd[@]}";
    then
        echo "Error occurred when executing deploy pc infra, please retry again. Exiting..."
        exit 66
    fi
}

function main() {
  undeploy_old_infra
  deploy_pc_infra
  echo "upgrade to pc infra has completed successfully."
  exit 0
}

main "$@"
