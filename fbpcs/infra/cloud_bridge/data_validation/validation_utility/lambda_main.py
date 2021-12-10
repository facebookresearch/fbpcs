# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import urllib
from datetime import datetime

import boto3
import validation

DEBUG_MODE = str(os.environ.get("VALIDATION_DEBUG_MODE")) == "1"
DEFAULT_VALIDATION_RESULTS_S3_KEY = "events-validation-results"


def debug_log(msg: str) -> None:
    if DEBUG_MODE:
        print(f"{msg}\n")


def write_result_to_s3(
    bucket_name: str, bucket_key: str, file_name: str, file_contents: str
) -> None:
    debug_log(
        "\n".join(
            [
                "write_result_to_s3:",
                f"bucket_name: {bucket_name}",
                f"bucket_key: {bucket_key}",
                f"file_name: {file_name}",
                f"file_contents: {file_contents}",
            ]
        )
    )
    s3_client = boto3.client("s3")
    key = bucket_key + "/" + file_name
    response = s3_client.put_object(
        Body=file_contents.encode("utf-8"), Bucket=bucket_name, Key=key
    )
    debug_log(response)


def validate_and_generate_report(bucket: str, key: str) -> str:
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        return validation.generate_from_body(body)
    except BaseException as e:
        error_message = f"Something went wrong while validating the data. Exception details if available:\n{e}"
        debug_log(error_message)
        return error_message


# context type is: awslambdaric.lambda_context.LambdaContext
def lambda_handler(event, context):
    output_bucket = os.environ.get("UPLOAD_AND_VALIDATION_S3_BUCKET")
    if not output_bucket:
        raise Exception(f"Exception: missing output_bucket: `{output_bucket}`")

    output_key_override = os.environ.get("VALIDATION_RESULTS_S3_KEY")

    debug_log(
        f"output bucket: {output_bucket}\noutput key override: {output_key_override}"
    )

    try:
        for record in event["Records"]:
            input_bucket = record["s3"]["bucket"]["name"]
            input_key = urllib.parse.unquote_plus(
                record["s3"]["object"]["key"], encoding="utf-8"
            )
            debug_log(f"input bucket: {input_bucket}\ninput key: {input_key}")
            validation_results = validate_and_generate_report(input_bucket, input_key)

            path_split = input_key.split("/")
            input_filename = path_split[-1]
            input_key_path = "/".join(path_split[:-1])

            if output_key_override:
                output_key = f"{input_key_path}/{output_key_override}"
            else:
                output_key = f"{input_key_path}/{DEFAULT_VALIDATION_RESULTS_S3_KEY}"

            debug_log(f"output key: {output_key}")

            validation_result_file_path = "_".join(
                [
                    input_filename,
                    "validation-results",
                    datetime.now().isoformat(),
                ]
            )

            write_result_to_s3(
                output_bucket,
                output_key,
                validation_result_file_path,
                validation_results,
            )
    except BaseException as e:
        print(f"Unexpected error occurred: {e}")

    debug_log("Finished validation processing")
    return {"records": []}
