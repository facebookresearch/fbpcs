# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import io
import os
import shutil

from dataclasses import dataclass
from enum import Enum
from pprint import pprint
from typing import Any, Dict, List, Union

from fbpcs.infra.logging_service.download_logs.cloud_error.utils_error import (
    NotSupportedContentType,
)


class Utils:
    def create_file(
        self, file_location: str, content: Union[List[str], Dict[str, Any]]
    ) -> None:
        """
        Create file in the file location with content.
        Args:
            file_location (str): Full path of the file location Eg: /tmp/xyz.txt
            content (list): Content to be written in file
        Returns:
            None
        """
        content = content or []

        try:
            # write to a file, if it already exists
            with open(file_location, "w") as file_object:
                self.write_to_file(file_object, content)
        except IOError as error:
            # T122918736 - for better execption messages
            raise Exception(f"Failed to create file {file_location}") from error

    @classmethod
    def write_to_file(
        cls,
        file_object: io.TextIOWrapper,
        contents: Union[List[str], Dict[str, Any]],
        append_newline: bool = True,
    ) -> None:
        """
        Write content to the file.
        Args:
            file_object (IO): Object of file to read/write
            contents (list): Content to be written in file
        Returns:
            None
        """
        if isinstance(contents, list):
            for content in contents:
                if append_newline:
                    content = content + "\n"
                file_object.write(content)
        elif isinstance(contents, dict):
            pprint(contents, stream=file_object)
            file_object.write("\n")
        else:
            raise NotSupportedContentType(
                "Unable to write to the file. Content type not supported."
            )

    @staticmethod
    def create_folder(folder_location: str) -> None:
        """
        Creates folder in the given path
        Args:
            folder_location (str): Path were folder will be created. Path includes new folder name.
                                   Eg: If creating folder `test` in location `/tmp`, folder_location should be `/tmp/test`
        Returns:
            None
        """

        if not os.path.exists(folder_location):
            os.makedirs(folder_location)

    @staticmethod
    def compress_downloaded_logs(folder_location: str) -> None:
        """
        Compresses folder passed to the function in arguments
        Args:
            folder_location (str): Complete folder path Eg /tmp/folder1
        """
        if os.path.isdir(folder_location):
            shutil.make_archive(folder_location, "zip", folder_location)
        else:
            # T122918736 - for better exception messages
            raise Exception(
                f"Couldn't find folder {folder_location}."
                f"Please check if folder exists.\nAborting folder compression."
            )

    @staticmethod
    def copy_file(source: str, destination: str) -> str:
        """
        Copys folder from source to destination path.
        Returns path of the new file (which is same as destiantion)
        """
        try:
            file_path = shutil.copy2(src=source, dst=destination)
        except shutil.SameFileError as err:
            raise shutil.SameFileError(
                f"{source} and {destination} represents same file)"
            ) from err
        except PermissionError as err:
            raise PermissionError("Permission denied") from err
        return file_path

    @staticmethod
    def get_file_name_from_path(file_path: str) -> str:
        """
        Returns files name from a given filepath
        Eg: /tmp/something/nothing/everything.log
        Return: everything.log
        """
        _, file_name = os.path.split(file_path)
        return file_name

    @staticmethod
    def string_formatter(preset_string: str, *args: str) -> str:
        return preset_string.format(*args)


class StringFormatter(str, Enum):
    LOG_GROUP = "/{}/{}"
    LOG_STREAM = "{}/{}/{}"
    LOCAL_FOLDER_LOCATION = "/tmp/{}"
    LOCAL_ZIP_FOLDER_LOCATION = "{}.zip"
    FILE_LOCATION = "{}/{}"
    ZIPPED_FOLDER_NAME = "{}.zip"
    KINESIS_FIREHOSE_DELIVERY_STREAM = "cb-data-ingestion-stream-{}"
    LAMBDA_LOG_GROUP_NAME = "/aws/lambda/{}"


@dataclass
class ContainerDetails:
    service_name: str
    container_name: str
    container_id: str


class DeploymentLogFiles(str, Enum):
    DEPLOY_LOG = "/tmp/deploy.log"
    TERRAFORM_LOG = "/tmp/terraform.log"
    SERVER_LOG = "/tmp/server.log"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in DeploymentLogFiles]


class DataInfraLambda(str, Enum):
    STREAM_PROCESSING = "cb-data-ingestion-stream-processor-{}"
    SEMI_AUTOMATED = "manual-upload-trigger-{}"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in DataInfraLambda]
