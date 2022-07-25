#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import unittest
from unittest.mock import MagicMock, patch

from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN

from fbpcs.pl_coordinator.bolt_graphapi_client import (
    BoltGraphAPIClient,
    BoltPAGraphAPICreateInstanceArgs,
    BoltPLGraphAPICreateInstanceArgs,
)
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound

ACCESS_TOKEN = "access_token"
URL = "https://graph.facebook.com/v13.0"


class TestBoltGraphAPIClient(unittest.IsolatedAsyncioTestCase):
    @patch("logging.Logger")
    def setUp(self, mock_logger) -> None:
        self.mock_logger = mock_logger
        config = {"access_token": ACCESS_TOKEN}
        self.test_client = BoltGraphAPIClient(config, mock_logger)
        self.test_client._check_err = MagicMock()

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

    @patch("fbpcs.pl_coordinator.bolt_graphapi_client.requests.post")
    async def test_bolt_create_lift_instance(self, mock_post) -> None:
        test_pl_args = BoltPLGraphAPICreateInstanceArgs(
            instance_id="test_pl",
            study_id="study_id",
            breakdown_key={
                "cell_id": "cell_id",
                "objective_id": "obj_id",
            },
        )
        await self.test_client.create_instance(test_pl_args)
        mock_post.assert_called_once_with(
            f"{URL}/study_id/instances",
            params={
                "access_token": ACCESS_TOKEN,
                "breakdown_key": json.dumps(test_pl_args.breakdown_key),
            },
        )

    @patch("fbpcs.pl_coordinator.bolt_graphapi_client.requests.post")
    async def test_bolt_create_attribution_instance(self, mock_post) -> None:
        test_pa_args = BoltPAGraphAPICreateInstanceArgs(
            instance_id="test_pa",
            dataset_id="dataset_id",
            timestamp="0",
            attribution_rule="attribution_rule",
            num_containers="1",
        )
        await self.test_client.create_instance(test_pa_args)
        mock_post.assert_called_once_with(
            f"{URL}/dataset_id/instance",
            params={
                "access_token": ACCESS_TOKEN,
                "attribution_rule": "attribution_rule",
                "timestamp": "0",
            },
        )

    async def test_run_stage(self) -> None:
        pass

    async def test_update_instance(self) -> None:
        pass

    async def test_validate_results(self) -> None:
        pass
