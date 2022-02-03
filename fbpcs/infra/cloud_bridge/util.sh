#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# shellcheck disable=SC2155
# Warning in validate_or_create_s3_bucket: Declare and assign separately to avoid masking return values.

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

validate_or_create_s3_bucket() {
    local bucket_name=$1
    local region=$2
    local aws_account_id=$3
    echo "######################## Create S3 buckets if don't exist ########################"
    local tmp=$(aws s3api head-bucket --bucket "$bucket_name" --expected-bucket-owner "$aws_account_id" 2>&1)
    local error=$(echo "$tmp" | grep -o '40[034]')
    if [ -z "$error" ];
    then
        echo "The bucket $bucket_name already exists and you have access to it. Continue..."
    elif [ "$error" -eq "404" ]
    then
        echo "The bucket $bucket_name doesn't exist. Creating..."
        # Creating S3 bucket in regions other than "us-east-1" needs the LocationConstraint field.
        # Ref: https://github.com/boto/boto3/issues/125
        if [ "$region" = "us-east-1" ]
        then
            aws s3api create-bucket --bucket "$bucket_name" --region "$region" || exit 1
        else
            aws s3api create-bucket --bucket "$bucket_name" --region "$region" --create-bucket-configuration LocationConstraint="$region" || exit 1
        fi
        aws s3api put-bucket-versioning --bucket "$bucket_name" --versioning-configuration Status=Enabled
        aws s3api put-bucket-encryption --bucket "$bucket_name" --server-side-encryption-configuration '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"},"BucketKeyEnabled": true}]}'
        aws s3api put-bucket-policy --bucket "$bucket_name" --policy "{\"Statement\": [{\"Effect\": \"Deny\",\"Action\": \"s3:*\",\"Principal\": \"*\",\"Resource\": [\"arn:aws:s3:::${bucket_name}\",\"arn:aws:s3:::${bucket_name}/*\"],\"Condition\": {\"Bool\": {\"aws:SecureTransport\": false }}}]}"
        echo "The bucket $bucket_name is created."

    elif [ "$error" -eq "400" ]
    then
        echo "Bad request when calling the HeadBucket operation."
        echo "Are you trying to reuse a recently deleted bucket? Please try to use a new bucket name, or tag."
        exit 1

    elif [ "$error" -eq "403" ] # no access to the bucket
    then
        echo "the bucket $bucket_name is owned by a different account."
        echo "Please check your whether your AWS account id $aws_account_id matches your secret key and access key provided"
        exit 1
    fi
}

validate_bucket_name() {
    # reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    local bucket_name=$1
    if [ "${bucket_name:0:4}" == "xn--" ]
    then
        echo "Error: invalid bucket name. Bucket names must not start with the prefix xn--"
        exit 1
    fi

    if [ "${bucket_name: -8}" == "-s3alias" ]
    then
        echo "Error: invalid bucket name. Bucket names must not end with the suffix -s3alias."
        exit 1
    fi
    aws_regex="^([a-z0-9][a-z0-9-]{1,61}[a-z0-9])$"
    if echo "$bucket_name" | grep -Eq "$aws_regex"
    then
        echo "Valid bucket name. Continue..."
    else
        echo "Error: invalid bucket name. please check out bucket naming rules at: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html"
        echo "Additionally, although valid, using dots is unrecommended by Amazon and as such we do not allow it."
        exit 1
    fi

}

validateDeploymentResources () {
    local region=$1
    local pce_id=$2
    local successMessage="Your PCE environments are set up correctly."
    echo "##### validating through PCE validator starts"
    local pceValidatorOutput=$(python3 -m pce.validator --region="$region" --key-id="$AWS_ACCESS_KEY_ID" --key-data="$AWS_SECRET_ACCESS_KEY" --pce-id="$pce_id" 2>&1)
    echo "$pceValidatorOutput"
    if echo "$pceValidatorOutput" | grep -q "$successMessage"
    then
        echo "PCE validation successful";
    else
        echo "PCE validator found some issue..please analyze further to debug the issue"
        # TODO, once T111250475 is implemneted grep based on exit code
        # exit 1
    fi
    echo "##### validating through PCE validator end"
}



input_validation () {
    local region=$1
    local tag_postfix=$2
    local aws_account_id=$3
    local publisher_aws_account_id=$4
    local publisher_vpc_id=$5
    local s3_bucket_for_storage=$6
    local s3_bucket_data_pipeline=$7
    local build_semi_automated_data_pipeline=$8
    local undeploy=$9
    echo "######################input validation############################"

        echo "validate AWS credential..."
    if aws sts get-caller-identity 2>&1 | grep -q "error" # InvalidClientTokenId or SignatureDoesNotMatch
    then
        echo "Error: AWS credential is invalid. Check your AWS Access Key ID, Secret Access Key, and signing method"
        exit 1
    elif aws sts get-caller-identity 2>&1 | grep -q "Could not connect to the endpoint URL"
    then
        echo "AWS auth request failed, either due to a networking error or incorrect region."
        echo "Please check out AWS User Guide on Available Regions: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html"
        exit 1
    else
        echo "Valid AWS credential. Continue..."
    fi

    echo "validate input: AWS region..."
    echo "Your AWS region is $region."
    # using region us-west-1 to fetch all available regions
    valid_region_list=$(aws ec2 describe-regions \
        --region us-west-1 \
        --query "Regions[].{Name:RegionName}" \
        --output text)
    if echo "$valid_region_list" | grep -q "$region"
    then
        echo "valid AWS region."
    else
        echo "invalid AWS region. Please check out AWS User Guide on Available Regions: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html"
        exit 1
    fi




    echo "validate input: tag..."
    tag_regex="^([a-z0-9][a-z0-9-]{1,18}[a-z0-9])$" # limit tag to 20 characters
    echo "The string '$tag_postfix' will be appended after the tag of the AWS resources."
    if echo "$tag_postfix" | grep -Eq "$tag_regex"
    then
        echo "valid tag. Continue..."
    else
        echo "Error: invalid tag format."
        echo "Making sure the tag length is less than 20 characters, using lowercase letters, numbers and dash only (but not starting with dash)."
        exit 1
    fi

    echo "Publisher's AWS account ID is $publisher_aws_account_id"
    echo "Publisher's VPC ID is $publisher_vpc_id"
    echo "validate input: s3 buckets..."
    echo "The S3 bucket for storing 1) Terraform state file, 2) AWS Lambda functions, and 3) config.yml is $s3_bucket_for_storage"
    validate_bucket_name "$s3_bucket_for_storage"
    echo "The S3 bucket for storing processed data is $s3_bucket_data_pipeline".
    validate_bucket_name "$s3_bucket_data_pipeline"

    if ! "$undeploy"
    then
        echo "making sure $s3_bucket_data_pipeline is not an existing bucket..."
        if aws s3api head-bucket --bucket "$s3_bucket_data_pipeline" --expected-bucket-owner "$aws_account_id" 2>&1 | grep -q "404" # bucekt doesn't exist
        then
            echo "The bucket $s3_bucket_data_pipeline doesn't exist. Continue..."
        else # bucket exists, we want the data-storage bucket to be new
            echo "The bucket $s3_bucket_data_pipeline already exists under Account $aws_account_id. Please choose another bucket name."
            exit 1
        fi
    fi
    echo "validate input: aws account id..."
    echo "Your AWS acount ID is $aws_account_id"
    account_A=$(aws sts get-caller-identity |grep -o 'Account":.*' | tr -d '"' | tr -d ' ' | tr -d ',' | cut -d':' -f2)
    account_B=$aws_account_id
    if [ "$account_A" == "$account_B" ]
    then
        echo "input AWS account is valid."
    else # not equal
        echo "Error: the provided AWS account id does not match the configured [secret_key, access_key]"
        exit 1
    fi

    echo "validate input: build semi automated data pipeline..."
    echo "build semi automated data pipeline: $build_semi_automated_data_pipeline"
    if [ "$build_semi_automated_data_pipeline" = "true" ] || [ "$build_semi_automated_data_pipeline" = "false" ]
    then
        echo "build_semi_automated_data_pipeline is valid."
    else
        echo "Error: input for build_semi_automated_data_pipeline is invalid. please provide a value of true|false."
        exit 1
    fi
}
