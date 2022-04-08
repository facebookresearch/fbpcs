#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

##########################################
# Helper functions
##########################################

verify_object_existence() {
    local bucket_name=$1
    local key_name=$2

    missing=$("gsutil ls gs://${bucket_name}/${key_name} |& grep -c CommandException")

    if [ "${missing}" == 1 ]; then
        echo "The file $key_name does not exist. Exiting..."
        false
    else
        echo "The file $key_name exists."
        true
    fi
}

check_bucket_exists() {
    local bucket_name=$1

    missing=$(gsutil ls gs://"${bucket_name}" |& grep -c BucketNotFound)

    if [ "${missing}" == 1 ]; then
        false
    else
        true
    fi
}

create_gcs_bucket() {
    local bucket_name=$1
    local region=$2
    gsutil mb -l "$region" "gs://$bucket_name"
}

verify_gcs_bucket_access () {
    local bucket_name=$1

    no_access=$(gsutil ls gs://"${bucket_name}" |& grep -c AccessDeniedException)

    if [ "${no_access}" == 1 ]; then
        false
    else
        true
    fi
}

verify_or_create_bucket() {
    local bucket_name=$1
    local region=$2
    echo "########################Create storage buckets if they don't exist ########################"

    if ! check_bucket_exists "$bucket_name";
    then
        echo "The bucket $bucket_name doesn't exist. Creating..."
        create_gcs_bucket "$bucket_name" "$region"
        echo "The bucket $bucket_name is created."
        true
    elif ! verify_gcs_bucket_access "$bucket_name"
    then
        echo "You don't have access to the bucket $bucket_name. Please choose another name."
        false
    else
        echo "The bucket $bucket_name exists and you have access to it."
        true
    fi
}
