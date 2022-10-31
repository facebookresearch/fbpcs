# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch

from fbpcs.private_computation import pc_attribution_runner
from fbpcs.private_computation.entity.product_config import AttributionRule


class TestPcAttributionRunner(TestCase):
    @patch(
        "fbpcs.private_computation.pc_attribution_runner.BoltGraphAPIClient",
        new=MagicMock(),
    )
    @patch(
        "fbpcs.private_computation.pc_attribution_runner.datetime",
    )
    def test_get_runnable_timestamps(self, mock_datetime: MagicMock) -> None:
        mock_datetime.now = MagicMock(
            return_value=datetime(2022, 10, 27, 23, 27, 55, 204663, tzinfo=timezone.utc)
        )

        dataset_id = "123"
        target_id = "456"

        datasets_info = {
            "datasets_information": [
                {
                    "key": "LAST_CLICK_1D",
                    "value": [
                        # in progress run, not a runnable timestamp
                        {
                            "timestamp": "2022-03-11T00:00:00+0000",
                            "status": "PROCESSING_REQUEST",
                            "hmac_key": "test",
                            "num_rows": 1,
                        },
                        # expired instance, runnable timestamp
                        {
                            "timestamp": "2022-03-13T00:00:00+0000",
                            "status": "PCF2_AGGREGATION_FAILED",
                            "hmac_key": "test",
                            "num_rows": 1,
                        },
                        # terminal state, runnable timestamp
                        {
                            "timestamp": "2022-05-10T07:00:00+0000",
                            "status": "RESULT_READY",
                            "hmac_key": "test",
                            "num_rows": 4800,
                        },
                        # no existing instance, runnable timestamp
                        {
                            "timestamp": "2022-03-14T00:00:00+0000",
                            "status": "Created",
                            "hmac_key": "test",
                            "num_rows": 1,
                        },
                    ],
                },
            ],
            "target_id": target_id,
            "id": dataset_id,
        }

        instance_data = {
            "data": [
                {
                    # in progress run, not a runnable timestamp
                    "id": "1",
                    "status": "PROCESSING_REQUEST",
                    "attribution_rule": "last_click_1d",
                    "created_time": "2022-10-27T21:42:24+0000",
                    "num_containers": 2,
                    "num_shards": 2,
                    "timestamp": "2022-03-11T00:00:00+0000",
                    "tier": "private_measurement.private_computation_service_rc",
                    "feature_list": [
                        "num_mpc_container_mutation",
                        "private_computation_coordinated_retry",
                    ],
                },
                {
                    # expired instance, runnable timestamp
                    "id": "2",
                    "status": "PCF2_AGGREGATION_FAILED",
                    "attribution_rule": "last_click_1d",
                    "server_ips": ["10.0.12.216"],
                    "created_time": "2022-10-26T00:44:20+0000",
                    "num_containers": 2,
                    "num_shards": 2,
                    "timestamp": "2022-03-13T00:00:00+0000",
                    "tier": "private_measurement.private_computation_service_rc",
                    "feature_list": [
                        "num_mpc_container_mutation",
                        "private_computation_coordinated_retry",
                    ],
                },
                {
                    # terminal state, runnable timestamp
                    "id": "3",
                    "status": "RESULT_READY",
                    "attribution_rule": "last_click_1d",
                    "server_ips": ["10.0.27.77"],
                    "created_time": "2022-10-27T21:42:24+0000",
                    "num_containers": 2,
                    "num_shards": 2,
                    "timestamp": "2022-05-10T07:00:00+0000",
                    "tier": "private_measurement.private_computation_service_rc",
                    "feature_list": [
                        "num_mpc_container_mutation",
                        "private_computation_coordinated_retry",
                    ],
                },
            ],
        }

        pc_attribution_runner._get_attribution_dataset_info = MagicMock(
            return_value=datasets_info
        )
        pc_attribution_runner._get_existing_pa_instances = MagicMock(
            return_value=instance_data
        )

        expected_results = {
            "2022-03-13T00:00:00+0000",
            "2022-05-10T07:00:00+0000",
            "2022-03-14T00:00:00+0000",
        }

        actual_results = pc_attribution_runner.get_runnable_timestamps(
            config={},
            dataset_id=dataset_id,
            logger=logging.getLogger(__name__),
            attribution_rule=AttributionRule.LAST_CLICK_1D,
        )

        self.assertEqual(expected_results, actual_results)
