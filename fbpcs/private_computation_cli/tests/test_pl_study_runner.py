#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import random
from typing import Any, List
from unittest import TestCase
from unittest.mock import patch, PropertyMock

import requests

from fbpcs.pl_coordinator import pl_study_runner


class TestPlStudyRunner(TestCase):
    @patch("logging.Logger")
    @patch("fbpcs.pl_coordinator.pc_graphapi_utils.PCGraphAPIClient")
    def setUp(
        self,
        mock_graph_api_client,
        mock_logger,
    ) -> None:
        self.mock_graph_api_client = mock_graph_api_client
        self.mock_logger = mock_logger
        self.num_shards = 2
        self.cell_id = str(random.randint(100, 200))
        self.objective_id = str(random.randint(100, 200))
        self.instance_id = str(random.randint(100, 200))
        self.cell_obj_instances = {
            self.cell_id: {
                self.objective_id: {
                    "latest_data_ts": 1645039053,
                    "input_path": "https://input/path/to/input.csv",
                    "num_shards": self.num_shards,
                    "instance_id": self.instance_id,
                    "status": "CREATED",
                }
            }
        }

    def test_get_pcs_features(self) -> None:
        expected_features = ["dummy_feature1", "dummy_feature2"]
        self.mock_graph_api_client.get_instance.return_value = (
            self._get_graph_api_output("CREATED", expected_features)
        )
        tested_features = pl_study_runner._get_pcs_features(
            self.cell_obj_instances, self.mock_graph_api_client
        )
        self.assertEqual(tested_features, expected_features)

    def _get_graph_api_output(
        self, status: str, feature_list: List[str]
    ) -> requests.Response:
        data = {"status": status, "feature_list": feature_list}
        r = requests.Response()
        r.status_code = 200
        # pyre-ignore
        type(r).text = PropertyMock(return_value=json.dumps(data))

        def json_func(**kwargs) -> Any:
            return data

        r.json = json_func
        return r
