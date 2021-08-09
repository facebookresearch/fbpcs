#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import pathlib
import unittest
from unittest.mock import mock_open, patch

from fbpmp.utils import abstract_file_ctx


class TestAbstractFileCtx(unittest.TestCase):
    def setUp(self):
        os.environ["PL_AWS_REGION"] = "us-west-1"
        os.environ["PL_AWS_KEY_ID"] = "key"
        os.environ["PL_AWS_KEY_DATA"] = "key_data"

    @patch("fbpmp.utils.abstract_file_ctx.BufferedS3Reader")
    def test_abstract_file_reader_path(self, mock_s3_reader):
        # Check a local path
        local_path = pathlib.Path("/path/to/local_file.txt")
        res = abstract_file_ctx.abstract_file_reader_path(local_path)
        mock_s3_reader.assert_not_called()
        self.assertEqual(res, local_path)

        # Check an s3 path
        s3_path = pathlib.Path("https://bucket-name.s3.Region.amazonaws.com/key-name")
        res = abstract_file_ctx.abstract_file_reader_path(s3_path)
        mock_s3_reader.assert_called_once()
        self.assertNotEqual(res, s3_path)
        # Check the path was copied to local
        self.assertTrue(res.startswith("/"))

    @patch("fbpmp.utils.abstract_file_ctx.BufferedS3Writer")
    def test_abstract_file_writer_ctx(self, mock_s3_writer):
        # Check a local path
        local_path = pathlib.Path("/path/to/local_file.txt")
        with patch("builtins.open", mock_open()) as m:
            res = abstract_file_ctx.abstract_file_writer_ctx(local_path)
            # Easiest way to test for equality is to do a quick write
            res.write("abc")
            m().write.assert_called_once_with("abc")

        # Check an s3 path
        s3_path = pathlib.Path("https://bucket-name.s3.Region.amazonaws.com/key-name")
        res = abstract_file_ctx.abstract_file_writer_ctx(s3_path)
        # Easiest way to test for equality is to do a quick write
        res.write("xyz")
        mock_s3_writer().write.assert_called_once_with("xyz")
