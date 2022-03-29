# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import os
from typing import Optional, Dict, List

from fbpcp.error.pcp import PcpError
from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.pc_pre_validation.constants import (
    BINARY_REPOSITORY,
    BINARY_PATHS,
    BINARY_FILE_VALIDATOR_NAME,
    ONEDOCKER_REPOSITORY_PATH,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.pc_pre_validation.validator import Validator


class BinaryFileValidator(Validator):
    def __init__(
        self,
        region: str,
        binary_repository: str = BINARY_REPOSITORY,
        binary_paths: List[str] = BINARY_PATHS,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
    ) -> None:
        self._storage_service = S3StorageService(region, access_key_id, access_key_data)
        self._name: str = BINARY_FILE_VALIDATOR_NAME
        self._binary_repository = binary_repository
        self._binary_paths = binary_paths

    @property
    def name(self) -> str:
        return self._name

    def __validate__(self) -> ValidationReport:
        details: Dict[str, str] = {}
        if not os.getenv(ONEDOCKER_REPOSITORY_PATH):
            # Skip s3 binary check if ONEDOCKER_REPOSITORY_PATH envvar is set
            #   This is to unblock fbpcs-github-cicd tests because it uses local binaries
            #   and do not have s3 permissions to the s3 binaries
            for path in self._binary_paths:
                binary_full_path = f"{self._binary_repository}/{path}"
                try:
                    if not self._storage_service.file_exists(binary_full_path):
                        details[binary_full_path] = "binary does not exist"
                except PcpError as pcp_error:
                    # s3 throws the following error when an access is denied,
                    #    An error occurred (403) when calling the HeadObject operation: Forbidden
                    if "Forbidden" in str(pcp_error):
                        details[binary_full_path] = str(pcp_error)
                    else:
                        # rethrow unexpected error so validation runner will skip this validation with a WARNING message
                        raise pcp_error

        if details:
            return ValidationReport(
                validation_result=ValidationResult.FAILED,
                validator_name=self.name,
                message="You don't have permission to access some private computation softwares. Please contact your representative at Meta",
                details=details,
            )
        else:
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=self.name,
                message="Completed binary accessibility validation successfuly",
            )
