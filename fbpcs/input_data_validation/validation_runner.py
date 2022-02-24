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

import csv
import time
from typing import Dict, Optional

from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.input_data_validation.constants import INPUT_DATA_TMP_FILE_PATH
from fbpcs.input_data_validation.enums import ValidationResult
from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class ValidationRunner:
    def __init__(
        self,
        input_file_path: str,
        cloud_provider: CloudProvider,
        region: str,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        valid_threshold_override: Optional[str] = None,
    ) -> None:
        self._input_file_path = input_file_path
        self._local_file_path: str = self._get_local_filepath()
        self._cloud_provider = cloud_provider
        self._storage_service = S3StorageService(region, access_key_id, access_key_data)

    def _get_local_filepath(self) -> str:
        now = time.time()
        filename = self._input_file_path.split("/")[-1]
        return f"{INPUT_DATA_TMP_FILE_PATH}/{filename}-{now}"

    def run(self) -> Dict[str, str]:
        rows_processed_count = 0
        try:
            self._storage_service.copy(self._input_file_path, self._local_file_path)
            with open(self._local_file_path) as local_file:
                csv_reader = csv.DictReader(local_file)
                for _ in csv_reader:
                    rows_processed_count += 1
        except Exception as e:
            return self._format_validation_result(
                ValidationResult.FAILED,
                f"File: {self._input_file_path} failed validation. Error: {e}",
                rows_processed_count,
            )

        return self._format_validation_result(
            ValidationResult.SUCCESS,
            f"File: {self._input_file_path} was validated successfully",
            rows_processed_count,
        )

    def _format_validation_result(
        self, status: ValidationResult, message: str, rows_processed_count: int
    ) -> Dict[str, str]:
        return {
            "status": status.value,
            "message": message,
            "rows_processed_count": str(rows_processed_count),
        }
