# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import boto3

def validate_and_generate_report(bucket, key):
    report = ['Validation Summary:']
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    lines = 0
    for _ in body.iter_lines():
        lines += 1
    report.append('Total lines: %s' % lines)
    return '\n'.join(report) + '\n'
