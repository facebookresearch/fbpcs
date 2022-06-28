#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import argparse
import unittest
from unittest.mock import MagicMock, patch

from fbpcs.infra.cloud_bridge import cli


class TestCli(unittest.TestCase):
    def test_get_parser(self) -> None:
        actual = cli.get_parser()
        # There's really not a great way to test this beyond verifying
        # it doesn't throw an exception. Ideally we should add specific
        # tests to the underlying ParserBuilder classes>
        self.assertIsInstance(actual, argparse.ArgumentParser)

    @patch("fbpcs.infra.cloud_bridge.cli.AwsDeploymentHelperTool")
    def test_main(self, mock_deployment_helper_tool: MagicMock) -> None:
        m_inner = MagicMock()
        mock_deployment_helper_tool.return_value = m_inner
        with self.subTest("aws"):
            args = ["create", "aws"]
            cli.main(args)
            m_inner.create.assert_called_once()
            mock_deployment_helper_tool.assert_called_once()
        with self.subTest("gcp"):
            args = ["create", "gcp"]
            with self.assertRaises(SystemExit):
                cli.main(args)
