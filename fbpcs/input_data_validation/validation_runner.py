# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


"""
This is the main class that runs all of the validations.

This class handles the overall logic to:
* Copy the file to local storage
* Run the validations
* Generate a validation report

Error handling:
* If an unhandled error occurs, it will be returned in the report
"""

from typing import Optional

from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class ValidationRunner:
    def __init__(
        self,
        input_file_path: str,
        cloud_provider: CloudProvider,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        valid_threshold_override: Optional[str] = None,
    ) -> None:
        self._input_file_path = input_file_path
        self._cloud_provider = cloud_provider
        self._storage_service = S3StorageService(
            "us-west-1", access_key_id, access_key_data
        )
