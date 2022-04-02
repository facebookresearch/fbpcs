# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import os
from typing import Optional, Dict, List

from fbpcp.error.pcp import PcpError
from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.pc_pre_validation.binary_path import BinaryInfo, S3BinaryPath
from fbpcs.pc_pre_validation.constants import (
    DEFAULT_BINARY_REPOSITORY,
    DEFAULT_BINARY_VERSION,
    DEFAULT_EXE_FOLDER,
    BINARY_INFOS,
    BINARY_FILE_VALIDATOR_NAME,
    ONEDOCKER_REPOSITORY_PATH,
    ONEDOCKER_EXE_PATH,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.pc_pre_validation.validator import Validator


class BinaryFileValidator(Validator):
    def __init__(
        self,
        region: str,
        binary_infos: List[BinaryInfo] = BINARY_INFOS,
        binary_version: Optional[str] = None,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
    ) -> None:
        self._storage_service = S3StorageService(region, access_key_id, access_key_data)
        self._name: str = BINARY_FILE_VALIDATOR_NAME
        self._binary_infos = binary_infos
        self._binary_version: str = binary_version or DEFAULT_BINARY_VERSION
        self._repo_path: str = self._get_repo_path()
        self._exe_folder: str = self._get_exe_folder()

    @property
    def name(self) -> str:
        return self._name

    def __validate__(self) -> ValidationReport:
        repo_path = self._get_repo_path()

        if repo_path.upper() == "LOCAL":
            details = self._validate_local_binaries()
        else:
            details = self._validate_s3_binaries()

        return self._format_validation_report(details)

    def _get_repo_path(self) -> str:
        """Get binary repository path

        Returns:
            Return ONEDOCKER_REPOSITORY_PATH variable if set, return DEFAULT_BINARY_REPOSITORY otherwise.
        """
        repo_path = os.getenv(ONEDOCKER_REPOSITORY_PATH)
        return repo_path or DEFAULT_BINARY_REPOSITORY

    def _get_exe_folder(self) -> str:
        """Get the folder of local binaries

        Returns:
            Return ONEDOCKER_EXE_PATH variable if set, return DEFAULT_EXE_FOLDER otherwise.
        """
        exe_folder = os.getenv(ONEDOCKER_EXE_PATH)
        return exe_folder or DEFAULT_EXE_FOLDER

    def _validate_local_binaries(self) -> Dict[str, str]:
        """Validate the existence of local binaries
        Returns:
            A dictionary, representing the names of inaccessible binaries and the error reasons.
        """
        return {}

    def _validate_s3_binaries(self) -> Dict[str, str]:
        """Validate the existence of s3 binaries

        Returns:
            A dictionary, representing the names of inaccessible binaries and the error reasons.
        """
        details: Dict[str, str] = {}

        for binary_info in self._binary_infos:
            s3_binary_path: str = str(
                S3BinaryPath(self._repo_path, binary_info, self._binary_version)
            )
            try:
                if not self._storage_service.file_exists(s3_binary_path):
                    details[s3_binary_path] = "binary does not exist"
            except PcpError as pcp_error:
                # s3 throws the following error when an access is denied,
                #    An error occurred (403) when calling the HeadObject operation: Forbidden
                if "Forbidden" in str(pcp_error):
                    details[s3_binary_path] = str(pcp_error)
                else:
                    # rethrow unexpected error so validation runner will skip this validation with a WARNING message
                    raise pcp_error
        return details

    def _format_validation_report(self, details: Dict[str, str]) -> ValidationReport:
        """Create a validation report.

        Returns: a validation report, with detailed error reasons.
        """
        if details:
            return ValidationReport(
                validation_result=ValidationResult.FAILED,
                validator_name=self.name,
                message=f"You don't have permission to access some private computation software (Repo path: {self._repo_path}, software_version: {self._binary_version}). Please contact your representative at Meta",
                details=details,
            )
        else:
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=self.name,
                message=f"Completed binary accessibility validation successfully (Repo path: {self._repo_path}, software_version: {self._binary_version}).",
            )
