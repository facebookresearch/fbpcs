# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
from typing import IO, List


def create_file(self, file_location: str, content: List) -> None:
    """
    Create file in the file location with content.
    Args:
        file_location (str): Full path of the file location Eg: /tmp/xyz.txt
        content (list): Content to be written in file
    Returns:
        None
    """
    if not content:
        content = []
    try:
        # write to a file, if it already exists
        with open(file_location, "w") as file_object:
            self.write_to_file(file_object, content)
    except FileNotFoundError:
        # create a file if it doesn't exist and write to file
        with open(file_location, "x") as file_object:
            self.write_to_file(file_object, content)


@staticmethod
def write_to_file(file_object: IO[bytes], contents: List) -> None:
    """
    Write content to the file.
    Args:
        file_object (IO): Object of file to read/write
        contents (list): Content to be written in file
    Returns:
        None
    """
    for content in contents:
        file_object.write(content)


@staticmethod
def remove_file(file_location: str) -> None:
    """
    Remove file from the given file path
    Args:
        file_location (str): Full path of the file location Eg: /tmp/xyz.txt
    Returns:
        None
    """
    if os.path.isfile(file_location):
        os.remove(file_location)
