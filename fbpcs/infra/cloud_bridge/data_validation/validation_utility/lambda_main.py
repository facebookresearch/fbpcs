# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import boto3
import os
import urllib
import validation

from datetime import datetime

DEBUG_MODE = str(os.environ.get('VALIDATION_DEBUG_MODE')) == '1'

def debug_log(msg):
    if DEBUG_MODE:
        print('%s\n' % msg)

def write_result_to_s3(bucket_name, bucket_key, file_name, file_contents):
    debug_log(
        '\n'.join([
            'write_result_to_s3:',
            'bucket_name: %s' % bucket_name,
            'bucket_key: %s' % bucket_key,
            'file_name: %s' % file_name,
            'file_contents: %s' % file_contents,
        ])
    )
    s3_client = boto3.client('s3')
    key = bucket_key + '/' + file_name
    response = s3_client.put_object(
        Body=file_contents.encode('utf-8'),
        Bucket=bucket_name,
        Key=key
    )
    debug_log(response)

def lambda_handler(event, context):
    output_bucket_name = os.environ.get('UPLOAD_AND_VALIDATION_S3_BUCKET')
    output_bucket_key = os.environ.get('VALIDATION_RESULTS_S3_KEY')
    debug_log(
        'output bucket: %s\noutput key: %s' % (
            output_bucket_name,
            output_bucket_key,
        )
    )

    try:
        # todo: loop over all records
        record = event['Records'][0]
        input_bucket = record['s3']['bucket']['name']
        input_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        debug_log(
            'input bucket: %s\ninput key: %s' % (
                input_bucket,
                input_key
            )
        )
        validation_results = validation.validate_and_generate_report(input_bucket, input_key)

        input_filename = input_key.split('/')[-1]
        validation_result_file_path = '_'.join([
            input_filename,
            'validation-results',
            datetime.now().isoformat(),
        ])

        write_result_to_s3(
            output_bucket_name,
            output_bucket_key,
            validation_result_file_path,
            validation_results
        )
    except BaseException as e:
        print('Unexpected error occurred: %s' % e)

    debug_log('Finished validation processing')
    return {'records': []}
