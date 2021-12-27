#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import contextlib
import os
import pathlib

from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.utils.buffered_s3_file_handler import BufferedS3Reader, BufferedS3Writer


S3_PATH_DRIVE = "https:"


def abstract_file_reader_path(path: pathlib.Path) -> pathlib.Path:
    if path.parts[0].lower() == S3_PATH_DRIVE:
        region = os.environ.get("PL_AWS_REGION")
        key_id = os.environ.get("PL_AWS_KEY_ID")
        key_data = os.environ.get("PL_AWS_KEY_DATA")
        if region:
            storage_service = S3StorageService(
                region=region, access_key_id=key_id, access_key_data=key_data
            )
        else:
            storage_service = S3StorageService(
                access_key_id=key_id, access_key_data=key_data
            )
        with BufferedS3Reader(path, storage_service) as reader:
            return reader.copy_to_local()
    else:
        return pathlib.Path(path)


def abstract_file_writer_ctx(path: pathlib.Path) -> contextlib.AbstractContextManager:
    if path.parts[0].lower() == S3_PATH_DRIVE:
        region = os.environ.get("PL_AWS_REGION")
        key_id = os.environ.get("PL_AWS_KEY_ID")
        key_data = os.environ.get("PL_AWS_KEY_DATA")
        if region:
            storage_service = S3StorageService(
                region=region, access_key_id=key_id, access_key_data=key_data
            )
        else:
            storage_service = S3StorageService(
                access_key_id=key_id, access_key_data=key_data
            )
        return BufferedS3Writer(path, storage_service)
    else:
        return open(path, "w")
