#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch

from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN

from fbpcs.pl_coordinator.bolt_graphapi_client import BoltGraphAPIClient
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound

ACCESS_TOKEN = "access_token"


class TestBoltGraphAPIClient(unittest.IsolatedAsyncioTestCase):
    @patch("logging.Logger")
    def setUp(self, mock_logger) -> None:
        self.mock_logger = mock_logger

    def test_get_graph_api_token_from_dict(self) -> None:
        expected_token = "from_dict"
        config = {"access_token": expected_token}
        actual_token = BoltGraphAPIClient(config, self.mock_logger).access_token
        self.assertEqual(expected_token, actual_token)

    def test_get_graph_api_token_from_env_config_todo(self) -> None:
        expected_token = "from_env"
        with patch.dict("os.environ", {FBPCS_GRAPH_API_TOKEN: expected_token}):
            config = {"access_token": "TODO"}
            actual_token = BoltGraphAPIClient(config, self.mock_logger).access_token
            self.assertEqual(expected_token, actual_token)

    def test_get_graph_api_token_from_env_config_no_field(self) -> None:
        expected_token = "from_env"
        with patch.dict("os.environ", {FBPCS_GRAPH_API_TOKEN: expected_token}):
            config = {"random_field": "not_a_token"}
            actual_token = BoltGraphAPIClient(config, self.mock_logger).access_token
            self.assertEqual(expected_token, actual_token)

    def test_get_graph_api_token_dict_and_env(self) -> None:
        expected_token = "from_dict"
        with patch.dict("os.environ", {FBPCS_GRAPH_API_TOKEN: "from_env"}):
            config = {"access_token": expected_token}
            actual_token = BoltGraphAPIClient(config, self.mock_logger).access_token
            self.assertEqual(expected_token, actual_token)

    def test_get_graph_api_token_no_token_todo(self) -> None:
        config = {"access_token": "TODO"}
        with self.assertRaises(GraphAPITokenNotFound):
            BoltGraphAPIClient(config, self.mock_logger).access_token

    def test_get_graph_api_token_no_field(self) -> None:
        config = {"random_field": "not_a_token"}
        with self.assertRaises(GraphAPITokenNotFound):
            BoltGraphAPIClient(config, self.mock_logger).access_token

    async def test_create_instance(self) -> None:
        pass

    async def test_run_stage(self) -> None:
        pass

    async def test_update_instance(self) -> None:
        pass

    async def test_validate_results(self) -> None:
        pass
