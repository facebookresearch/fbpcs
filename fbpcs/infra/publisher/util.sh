#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

##########################################
# Helper functions
##########################################

check_s3_object_exist() {
    local bucket_name=$1
    local key_name=$2
    local account_id=$3
    aws s3api head-object --bucket "$bucket_name" --key "$key_name" --expected-bucket-owner "$account_id" || not_exist=true
    if [ $not_exist ]; then
        echo "The file $key_name does not exist. Exiting..."
        false
    else
        echo "The file $key_name exists."
        true
    fi
}

create_s3_bucket() {
    local bucket_name=$1
    local region=$2
    local aws_account_id=$3
    echo "########################Create S3 buckets if doesn't exist ########################"
    if aws s3api head-bucket --bucket "$bucket_name" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "404" # bucket doesn't exist
    then
        echo "The bucket $bucket_name doesn't exist. Creating..."
        aws s3api create-bucket --bucket "$bucket_name" --region "$region" --create-bucket-configuration LocationConstraint="$region" || exit 1
        aws s3api put-bucket-versioning --bucket "$bucket_name" --versioning-configuration Status=Enabled
        echo "The bucket $bucket_name is created."
        true
    elif aws s3api head-bucket --bucket "$bucket_name" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "403" # no access to the bucket
    then
        echo "You don't have access to the bucket $bucket_name. Please choose another name."
        false
    else
        echo "The bucket $bucket_name exists and you have access to it."
        true
    fi
}
