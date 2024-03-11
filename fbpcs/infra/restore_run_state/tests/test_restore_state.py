#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe


from unittest import mock, TestCase
from unittest.mock import ANY

from fbpcs.infra.restore_run_state.restore_state import RestoreRunState


class TestRestoreState(TestCase):
    def setUp(self) -> None:
        self.restore_state = RestoreRunState()

    def test_split_path(self) -> None:
        with self.subTest("Valid path"):
            self.assertEqual(
                ("fb-pc-data-bucket-wnm6", "logging/logs_20220928T201923/last"),
                self.restore_state._split_path(
                    "s3://fb-pc-data-bucket-wnm6/logging/logs_20220928T201923/last"
                ),
            )
        with self.subTest("Valid path 2"):
            self.assertEqual(
                ("fb-pc-data-bucket-wnm6", "last"),
                self.restore_state._split_path("s3://fb-pc-data-bucket-wnm6/last"),
            )
        with self.subTest("Invalid path"):
            self.assertIsNone(
                self.restore_state._split_path("s3://fb-pc-data-bucket-wnm6"),
            )
        with self.subTest("Invalid path 2"):
            self.assertIsNone(
                self.restore_state._split_path(
                    "fb-pc-data-bucket-wnm6/logging/logs_20220928T201923/last"
                ),
            )

    def test_copy_files(self) -> None:
        objects = [
            mock.Mock(key="key1"),
            mock.Mock(key="key2/"),
        ]
        self.restore_state.s3 = mock.MagicMock()
        bucket = mock.MagicMock()
        bucket.objects = mock.MagicMock()
        bucket.objects.filter = mock.MagicMock(return_value=objects)
        self.restore_state.s3.Bucket = mock.MagicMock(return_value=bucket)

        self.restore_state._copy_files(
            "s3://fb-pc-data-bucket-wnm6/logging/last", "/tmp"
        )

        bucket.download_file.assert_called_once_with("key1", ANY)
