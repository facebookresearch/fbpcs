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
from typing import Any, Dict, List, Optional, Union

from fbpcs.common.service.pii_scrubber import PiiLoggingScrubber

from fbpcs.infra.logging_service.download_logs.cloud_error.utils_error import (
    NotSupportedContentType,
)


class Utils:
    def __init__(self) -> None:
        self.pii_scrubber: PiiLoggingScrubber = PiiLoggingScrubber()

    def create_file(
        self,
        file_location: str,
        content: Union[List[str], Dict[str, Any]],
        **kwargs: Dict[str, Any],
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
        pii_scrubber = kwargs.get("scrub_pii_data", True)

        if pii_scrubber:
            content = self.scrub_logs_content(content=content)

        try:
            # write to a file, if it already exists
            with open(file_location, "w") as file_object:
                self.write_to_file(file_object, content, **kwargs)
        except IOError as error:
            # T122918736 - for better execption messages
            raise Exception(f"Failed to create file {file_location}") from error

    @classmethod
    def write_to_file(
        cls,
        file_object: io.TextIOWrapper,
        contents: Union[List[str], Dict[str, Any]],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Write content to the file.
        Args:
            file_object (IO): Object of file to read/write
            contents (list): Content to be written in file
        Returns:
            None
        """
        append_newline = kwargs.get("append_newline", True)

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

    def scrub_logs_content(
        self,
        content: Union[
            List[str],
            Dict[str, Any],
        ],
    ) -> Union[List[str], Dict[str, Any],]:
        """
        Calls other scrub functions based on the content type
        """
        if isinstance(content, dict):
            """
            Case 1: Content type dictionary
            content = {
                name: test
                email: test@test.com
                phone: +1-(123)-456-1234
            }
            """
            self.scrub_dict_content(content)
        elif isinstance(content, list):
            """
            Case 2: Content type list
            content = ["test", "test@test.com", "+1-(123)-456-1234"]
            """
            self.scrub_list_content(content)
        elif isinstance(content, (str, int)):
            """
            Case 3: Content type string or int
            content = "test email is test@test.com"
            """
            content = self.scrub_str_content(content=str(content))
        else:
            raise NotSupportedContentType("Not supported content type to scrub")
        return content

    def scrub_dict_content(self, content: Dict[str, Any]) -> None:
        """
        Scrubs dictionary content type
        In case of nested dictionary, function is recursively called
        """
        for key, value in content.items():
            if isinstance(value, dict):
                self.scrub_dict_content(value)
            elif isinstance(value, list):
                self.scrub_list_content(value)
            elif isinstance(value, (str, int)):
                content[key] = self.scrub_str_content(content=str(value))

    def scrub_list_content(
        self,
        content: List[str],
    ) -> None:
        """
        Scrubs list content type
        In case of nested lists, function is recursively called
        """
        for index in range(len(content)):
            if isinstance(content[index], dict):
                # pyre-fixme[6]
                self.scrub_dict_content(content[index])
            elif isinstance(content[index], list):
                # pyre-fixme[6]
                self.scrub_list_content(content[index])
            elif isinstance(content[index], (str, int)):
                content[index] = self.scrub_str_content(content=str(content[index]))

    def scrub_str_content(self, content: str) -> str:
        """
        Scrubs string content type
        """
        scrubber_object = self.pii_scrubber.scrub(content)
        scrubbed_content = scrubber_object.scrubbed_output
        return scrubbed_content

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
    def string_formatter(preset_string: str, *args: Optional[str]) -> str:
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
    GLUE_CRAWLER_NAME = "mpc-events-crawler-{}"
    GLUE_ETL_NAME = "glue-ETL-{}"
    ATHENA_DATABASE = "mpc-events-db-{}"


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
