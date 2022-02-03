#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import pathlib
import unittest
from unittest.mock import Mock

from fbpcs.utils.buffered_s3_file_handler import BufferedS3Reader, BufferedS3Writer


class TestBufferedS3Reader(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_path = pathlib.Path("https://bucket.s3.Region.amazonaws.com/object")
        self.storage_service = Mock()

    def test_context_manager(self) -> None:
        pass

    def test_seek(self) -> None:
        reader = BufferedS3Reader(self.s3_path, self.storage_service)
        reader.data = "x" * 100
        self.assertEqual(0, reader.cursor)

        reader.seek(50)
        self.assertEqual(50, reader.cursor)

        # Seek past the end of the data
        reader.seek(150)
        self.assertEqual(100, reader.cursor)

    def test_read(self) -> None:
        reader = BufferedS3Reader(self.s3_path, self.storage_service)
        reader.data = "x" * 100

        # Simple read
        res = reader.read(10)
        self.assertEqual("x" * 10, res)

        # Read more characters than available
        reader.seek(0)
        res = reader.read(1000)
        self.assertEqual(reader.data, res)

        # Read all data
        reader.seek(0)
        res = reader.read()
        self.assertEqual(reader.data, res)

        # Read all data after a partial read
        reader.seek(50)
        res = reader.read()
        self.assertEqual("x" * 50, res)

    def test_copy_to_local(self) -> None:
        reader = BufferedS3Reader(self.s3_path, self.storage_service)
        reader.data = "x" * 100
        temp_path = reader.copy_to_local()
        with open(temp_path) as f:
            content = f.read()

        self.assertEqual(reader.data, content)
        # The caller is responsible for cleaning up the temporary file
        os.unlink(temp_path)


class TestBufferedS3Writer(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_path = pathlib.Path("https://bucket.s3.Region.amazonaws.com/object")
        self.storage_service = Mock()

    def test_context_manager(self) -> None:
        with BufferedS3Writer(self.s3_path, self.storage_service) as writer:
            writer.write("abc")
        self.storage_service.write.assert_called_once_with(str(self.s3_path), "abc")

    def test_del(self) -> None:
        writer = BufferedS3Writer(self.s3_path, self.storage_service)
        writer.write("abc")
        # __del__ should be invoked by removing the only reference to writer
        writer = None
        self.storage_service.write.assert_called_once_with(str(self.s3_path), "abc")

    def test_write(self) -> None:
        writer = BufferedS3Writer(self.s3_path, self.storage_service)
        writer.write("abc")
        self.assertEqual("abc", writer.buffer)
