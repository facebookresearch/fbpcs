# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import shutil
import unittest
from unittest.mock import mock_open, patch

from fbpcs.infra.logging_service.download_logs.utils.utils import Utils


class TestUtils(unittest.TestCase):
    def setUp(self) -> None:
        self.utils = Utils()

    def test_create_file(self) -> None:

        fake_file_path = "fake/file/path"
        content_list = ["This is test string"]
        with patch(
            "fbpcs.infra.logging_service.download_logs.utils.utils.open",
            mock_open(),
        ) as mocked_file:
            with self.subTest("basic"):
                self.utils.create_file(
                    file_location=fake_file_path, content=content_list
                )
                mocked_file.assert_called_once_with(fake_file_path, "w")
                mocked_file().write.assert_called_once_with(content_list[0] + "\n")

            with self.subTest("ExceptionOpen"):
                mocked_file.side_effect = IOError()
                with self.assertRaisesRegex(Exception, "Failed to create file*"):
                    self.utils.create_file(
                        file_location=fake_file_path, content=content_list
                    )

    def test_write_to_file(self) -> None:
        # T124340651
        pass

    def test_create_folder(self) -> None:
        # T124340830
        pass

    def test_compress_downloaded_logs(self) -> None:
        # T124340929
        pass

    def test_copy_file(self) -> None:
        source = "source_path"
        destination = "destination_path"
        with patch("shutil.copy2") as mock_copy:
            with self.subTest("Test successful file copy"):
                mock_copy.return_value = destination
                result = self.utils.copy_file(source, destination)
                mock_copy.assert_called_once_with(src=source, dst=destination)
                self.assertEqual(result, destination)
            with self.subTest("Test SameFileError"):
                mock_copy.side_effect = shutil.SameFileError
                with self.assertRaisesRegex(
                    shutil.SameFileError,
                    f"{source} and {destination} represents same file",
                ):
                    self.utils.copy_file(source, destination)
            with self.subTest("Test PermissionError"):
                mock_copy.side_effect = PermissionError
                with self.assertRaisesRegex(PermissionError, "Permission denied"):
                    self.utils.copy_file(source, destination)
