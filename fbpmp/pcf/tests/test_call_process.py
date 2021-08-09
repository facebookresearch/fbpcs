#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import os
import pathlib
import unittest
from unittest.mock import Mock, patch

from fbpmp.pcf import call_process
from fbpmp.pcf.tests.async_utils import AsyncMock, wait


class TestCallProcess(unittest.TestCase):
    def test_read_stream(self):
        stream = Mock()
        stream.readline = AsyncMock(
            side_effect=[bytes("hello", "utf-8"), bytes("", "utf-8")]
        )
        logger = Mock(info=Mock())

        wait(call_process._read_stream(stream, "preamble", logger))
        logger.info.assert_called_once_with("preamble: hello")

    @patch(
        "asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=Mock(returncode=123)),
    )
    def test_run_command(self):
        logger = Mock(info=Mock(), warning=Mock(), debug=Mock())
        cwd = pathlib.Path(os.getcwd())
        res = wait(call_process.run_command(["a", "b", "c"], cwd, logger))
        self.assertEqual(123, res.returncode)
        asyncio.create_subprocess_exec.mock.assert_called_once_with(
            "a",
            "b",
            "c",
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    @patch(
        "asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=Mock(returncode=123)),
    )
    def test_run_commands(self):
        logger = Mock(info=Mock(), warning=Mock(), debug=Mock())
        logger2 = Mock(info=Mock(), warning=Mock(), debug=Mock())
        cwd = pathlib.Path(os.getcwd())
        res = wait(
            call_process.run_commands(
                [["a", "b", "c"], ["d", "e", "f"]], [cwd, cwd], [logger, logger2]
            )
        )
        self.assertEqual(2, len(res))
        self.assertEqual(123, res[0].returncode)
        self.assertEqual(123, res[1].returncode)
        self.assertEqual(2, asyncio.create_subprocess_exec.mock.call_count)
        asyncio.create_subprocess_exec.mock.assert_any_call(
            "a",
            "b",
            "c",
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        asyncio.create_subprocess_exec.mock.assert_any_call(
            "d",
            "e",
            "f",
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
