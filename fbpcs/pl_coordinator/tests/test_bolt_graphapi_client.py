#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import requests

from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN

from fbpcs.pl_coordinator.bolt_graphapi_client import (
    BoltGraphAPIClient,
    BoltPAGraphAPICreateInstanceArgs,
    BoltPLGraphAPICreateInstanceArgs,
)
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)

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
            run_id="run_id",
        )
        await self.test_client.create_instance(test_pl_args)
        mock_post.assert_called_once_with(
            f"{URL}/study_id/instances",
            params={
                "access_token": ACCESS_TOKEN,
                "breakdown_key": json.dumps(test_pl_args.breakdown_key),
                "run_id": test_pl_args.run_id,
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

    @patch("fbpcs.pl_coordinator.bolt_graphapi_client.requests.post")
    async def test_bolt_run_stage(self, mock_post) -> None:
        expected_params = {
            "access_token": ACCESS_TOKEN,
            "operation": "NEXT",
        }
        for stage in PrivateComputationStageFlow.ID_MATCH, None:
            mock_post.reset_mock()
            await self.test_client.run_stage(instance_id="id", stage=stage)
            mock_post.assert_called_once_with(f"{URL}/id", params=expected_params)

    @patch(
        "fbpcs.pl_coordinator.bolt_graphapi_client.BoltGraphAPIClient.get_instance",
        new_callable=AsyncMock,
    )
    async def test_bolt_update_instance(self, mock_get_instance) -> None:
        mock_get_instance.return_value = self._get_graph_api_output(
            {"id": "id", "status": "COMPUTATION_STARTED", "server_ips": "1.1.1.1"}
        )
        state = await self.test_client.update_instance("id")
        self.assertEqual(
            state.pc_instance_status,
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
        )
        self.assertEqual(state.server_ips, "1.1.1.1")

    async def test_validate_results_without_path(self) -> None:
        valid = await self.test_client.validate_results("id")
        self.assertEqual(valid, True)

    async def test_validate_results_with_path(self) -> None:
        expected_result_path = "test/path"
        with self.assertRaises(NotImplementedError):
            await self.test_client.validate_results("id", expected_result_path)

    @patch("fbpcs.bolt.bolt_job.BoltCreateInstanceArgs")
    @patch("fbpcs.bolt.bolt_client.BoltState")
    @patch(
        "fbpcs.pl_coordinator.bolt_graphapi_client.BoltGraphAPIClient.update_instance",
        new_callable=AsyncMock,
    )
    async def test_is_existing_instance(
        self, mock_update, mock_state, mock_args
    ) -> None:
        for instance_id, update_successful, expected_result in [
            ("", True, False),
            ("", False, False),
            ("id", True, True),
            ("id", False, False),
        ]:
            mock_update.side_effect = mock_state if update_successful else Exception()
            mock_args.instance_id = instance_id
            with self.subTest(
                instance_id=instance_id,
                update_successful=update_successful,
                expected_result=expected_result,
            ):
                actual_result = await self.test_client.is_existing_instance(mock_args)
                self.assertEqual(actual_result, expected_result)
                if not instance_id:
                    mock_update.assert_not_called()

    def _get_graph_api_output(self, text: Any) -> requests.Response:
        r = requests.Response()
        r.status_code = 200
        # pyre-ignore
        type(r).text = PropertyMock(return_value=json.dumps(text))

        def json_func(**kwargs) -> Any:
            return text

        r.json = json_func
        return r
