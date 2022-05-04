#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e
# shellcheck disable=SC1091
source ./util.sh

usage() {
    echo "Usage: deploy-workload-identity.sh <deploy|undeploy>
        [ -t, --tag | A unique identifier to identify resources in this deployment]
        [ -r, --region | GCP region, e.g. us-west-2 ]
        [ -a, --account_id | Publisher's GCP project ID]
        [ -b, --bucket | GCS bucket name for storing tfstate]"
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
        -t|--tag) pce_id="$2" ;;
        -r|--region) region="$2" ;;
        -a|--account_id) gcp_project_id="$2" ;;
        -b|--bucket) gcs_bucket_for_storage="$2" ;;
        *) usage ;;
    esac
    shift
    shift
done

name_postfix="${pce_id}"
cluster_name="onedocker-cluster-${name_postfix}"

echo "GCP region is $region."
echo "The string '$name_postfix' will be appended after the name of the GCP resources."
echo "Publisher's GCP project ID is $gcp_project_id"
echo "The GCS bucket for storing the Terraform state file is $gcs_bucket_for_storage"
echo "The cluster that workload identity will be applied on is $cluster_name"

undeploy_gcp_resources () {
    echo "Start undeploying..."
    echo "########################Check tfstate files########################"
    if ! verify_object_existence "$gcs_bucket_for_storage" "tfstate/pce$name_postfix.tfstate"; then
        exit 1
    fi
    echo "All tfstate files exist. Continue..."

    echo "########################Delete PCE resources########################"
    cd /terraform_deployment/terraform_scripts/common/workload_identity
    terraform init \
        -backend-config "bucket=$gcs_bucket_for_storage" \
        -backend-config "prefix=tfstate/wi-$name_postfix.tfstate"
    terraform destroy \
        -auto-approve \
        -var "gcp_region=$region" \
        -var "project_id=$gcp_project_id" \
        -var "name_postfix=$name_postfix" \
        -var "cluster_name=$cluster_name"
}

deploy_gcp_resources () {
    echo "########################Started GCP Infrastructure Deployment########################"
    verify_or_create_bucket "$gcs_bucket_for_storage" "$region"

    gcloud container clusters get-credentials --region "$region" "$cluster_name"

    # Deploy PCE Terraform scripts
    cd /terraform_deployment/terraform_scripts/common/workload_identity
    terraform init \
        -backend-config "bucket=$gcs_bucket_for_storage" \
        -backend-config "prefix=tfstate/wi-$name_postfix.tfstate"
    terraform apply \
        -auto-approve \
        -var "gcp_region=$region" \
        -var "project_id=$gcp_project_id" \
        -var "name_postfix=$name_postfix" \
        -var "cluster_name=$cluster_name"

    # Print the output
    echo "######################## PCE terraform output ########################"
    terraform output

    echo "########################Finished GCP Infrastructure Deployment########################"
}

##########################################
# Main
##########################################

if "$undeploy"
then
    echo "Undeploying the GCP resources..."
    undeploy_gcp_resources
else
    deploy_gcp_resources
fi
exit 0
