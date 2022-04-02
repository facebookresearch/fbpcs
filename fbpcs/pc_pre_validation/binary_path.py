# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import abc
from dataclasses import dataclass
from typing import Optional


class BinaryPath(abc.ABC):
    def __str__(self) -> str:
        return self._stringify()

    @abc.abstractmethod
    def _stringify(self) -> str:
        pass


@dataclass
class BinaryInfo:
    package: str
    binary: Optional[str] = None


class S3BinaryPath(BinaryPath):
    def __init__(
        self,
        repo_path: str,
        binary_info: BinaryInfo,
        version: str,
    ) -> None:
        self.repo_path: str = repo_path
        self.package: str = binary_info.package
        self.binary: str = binary_info.binary or binary_info.package.rsplit("/")[-1]
        self.version = version

    def _stringify(self) -> str:
        return f"{self.repo_path}{self.package}/{self.version}/{self.binary}"


class LocalBinaryPath(BinaryPath):
    def __init__(
        self,
        exe_folder: str,
        binary_info: BinaryInfo,
    ) -> None:
        self.exe_folder: str = exe_folder
        self.binary: str = binary_info.binary or binary_info.package.rsplit("/")[-1]

    def _stringify(self) -> str:
        return f"{self.exe_folder}{self.binary}"
