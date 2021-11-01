#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
from typing import Any, Optional, List, Tuple, Type
from unittest import TestCase
from unittest.mock import patch, PropertyMock

import requests
from fbpcs.pl_coordinator.pl_graphapi_utils import (
    GRAPHAPI_INSTANCE_STATUSES,
)
from fbpcs.pl_coordinator.pl_instance_runner import (
    PLInstanceRunner,
    PLInstanceCalculationException,
)
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_legacy_stage_flow import (
    PrivateComputationLegacyStageFlow,
)
from fbpcs.private_computation.entity.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)


class TestPlInstanceRunner(TestCase):
    @patch("logging.Logger")
    @patch("fbpcs.pl_coordinator.pl_graphapi_utils.PLGraphAPIClient")
    def setUp(
        self,
        mock_graph_api_client,
        mock_logger,
    ):
        self.mock_graph_api_client = mock_graph_api_client
        self.mock_logger = mock_logger
        self.num_shards = 2
        self.instance_id = "123"

    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PrivateLiftCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_ready_for_stage(self, mock_get_instance, mock_wait_valid_status) -> None:
        for (
            publisher_status,
            partner_status,
            stage,
            publisher_ready_for_stage_expected,
            partner_ready_for_stage_expected,
        ) in self._get_stage_ready_data():
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                stage=stage,
                publisher_ready_for_stage_expected=publisher_ready_for_stage_expected,
                partner_ready_for_stage_expected=partner_ready_for_stage_expected,
            ):
                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)

                runner = self._get_runner(type(stage))
                publisher_ready_for_stage_actual = runner.publisher.ready_for_stage(
                    stage
                )
                partner_ready_for_stage_actual = runner.partner.ready_for_stage(stage)
                self.assertEqual(
                    publisher_ready_for_stage_expected, publisher_ready_for_stage_actual
                )
                self.assertEqual(
                    partner_ready_for_stage_expected, partner_ready_for_stage_actual
                )

    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PrivateLiftCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_get_valid_stage(self, mock_get_instance, mock_wait_valid_status) -> None:
        for (
            publisher_status,
            partner_status,
            stage,
            _,
            _,
        ) in self._get_valid_stage_data():
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                stage=stage,
            ):
                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)

                runner = self._get_runner(
                    PrivateComputationLegacyStageFlow if not stage else type(stage)
                )
                runner.publisher.status = GRAPHAPI_INSTANCE_STATUSES[publisher_status]
                runner.partner.status = partner_status
                valid_stage = runner.get_valid_stage()
                self.assertEqual(valid_stage, stage)

    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PrivateLiftCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_should_invoke(self, mock_get_instance, mock_wait_valid_status) -> None:
        for (
            publisher_status,
            partner_status,
            stage,
            publisher_should_invoke_expected,
            partner_should_invoke_expected,
        ) in self._get_valid_stage_data():
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                stage=stage,
                publisher_should_invoke_expected=publisher_should_invoke_expected,
                partner_should_invoke_expected=partner_should_invoke_expected,
            ):
                # there is one row in the dataset that has no next stage
                if stage is None:
                    continue

                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)

                runner = self._get_runner(type(stage))
                publisher_should_invoke_actual = (
                    runner.publisher.should_invoke_operation(stage)
                )
                partner_should_invoke_actual = runner.partner.should_invoke_operation(
                    stage
                )
                self.assertEqual(
                    publisher_should_invoke_expected, publisher_should_invoke_actual
                )
                self.assertEqual(
                    partner_should_invoke_expected, partner_should_invoke_actual
                )

    @patch("fbpcs.pl_coordinator.pl_instance_runner.run_stage")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.aggregate_shards")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.compute_metrics")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.prepare_compute_input")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.id_match")
    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PrivateLiftCalcInstance.wait_stage_start"
    )
    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PLInstanceRunner.wait_stage_complete"
    )
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_run_stage(
        self,
        mock_get_instance,
        mock_wait_stage_complete,
        mock_wait_stage_start,
        mock_id_match,
        mock_prepare_compute_input,
        mock_compute_metrics,
        mock_aggregate_shards,
        mock_run_stage,
    ) -> None:
        for (
            publisher_status,
            partner_status,
            stage,
            publisher_should_invoke_expected,
            partner_should_invoke_expected,
        ) in self._get_valid_stage_data():
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                stage=stage,
                publisher_should_invoke_expected=publisher_should_invoke_expected,
                partner_should_invoke_expected=partner_should_invoke_expected,
            ):
                # there is one row in the dataset that has no next stage
                if stage is None:
                    continue

                mock_id_match.call_count = 0
                mock_prepare_compute_input.call_count = 0
                mock_compute_metrics.call_count = 0
                mock_aggregate_shards.call_count = 0
                mock_run_stage.call_count = 0
                mock_wait_stage_start.call_count = 0
                self.mock_graph_api_client.invoke_operation.call_count = 0

                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)

                runner = self._get_runner(type(stage))
                runner.run_stage(stage)

                if stage.is_joint_stage:
                    mock_wait_stage_start.assert_called()
                else:
                    mock_wait_stage_start.assert_not_called()

                if stage is PrivateComputationLegacyStageFlow.ID_MATCH:
                    if publisher_should_invoke_expected:
                        self.mock_graph_api_client.invoke_operation.assert_called_with(
                            self.instance_id, "ID_MATCH"
                        )
                    else:
                        self.mock_graph_api_client.invoke_operation.assert_not_called()

                    if partner_should_invoke_expected:
                        mock_id_match.assert_called()
                    else:
                        mock_id_match.assert_not_called()

                elif stage is PrivateComputationLegacyStageFlow.COMPUTE:
                    if publisher_should_invoke_expected:
                        self.mock_graph_api_client.invoke_operation.assert_called_with(
                            self.instance_id, "COMPUTE"
                        )
                    else:
                        self.mock_graph_api_client.invoke_operation.assert_not_called()

                    if partner_should_invoke_expected:
                        mock_prepare_compute_input.assert_called()
                        mock_compute_metrics.assert_called()
                    else:
                        mock_prepare_compute_input.assert_not_called()
                        mock_compute_metrics.assert_not_called()
                elif stage is PrivateComputationLegacyStageFlow.AGGREGATE:
                    if publisher_should_invoke_expected:
                        self.mock_graph_api_client.invoke_operation.assert_called_with(
                            self.instance_id, "AGGREGATE"
                        )
                    else:
                        self.mock_graph_api_client.invoke_operation.assert_not_called()

                    if partner_should_invoke_expected:
                        mock_aggregate_shards.assert_called()
                    else:
                        mock_aggregate_shards.assert_not_called()
                else:
                    if publisher_should_invoke_expected:
                        self.mock_graph_api_client.invoke_operation.assert_called_with(
                            self.instance_id, "NEXT"
                        )
                    else:
                        self.mock_graph_api_client.invoke_operation.assert_not_called()

                    if partner_should_invoke_expected:
                        mock_run_stage.assert_called()
                    else:
                        mock_run_stage.assert_not_called()

    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_wait_stage_start(self, mock_get_instance, mock_sleep) -> None:
        for (
            stage,
            statuses,
            result,
        ) in self._get_wait_stage_start_data():
            with self.subTest(
                stage=stage,
                statuses=statuses,
                result=result,
            ):
                self.mock_graph_api_client.get_instance.side_effect = (
                    self._get_graph_api_output(status) for status in statuses
                )
                mock_get_instance.return_value = self._get_pc_instance(
                    stage.previous_stage.completed_status
                )

                runner = self._get_runner(type(stage))
                if not result:
                    with self.assertRaises(PLInstanceCalculationException):
                        runner.publisher.wait_stage_start(stage)
                else:
                    runner.publisher.wait_stage_start(stage)

    @patch("fbpcs.pl_coordinator.pl_instance_runner.cancel_current_stage")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_wait_stage_completed(
        self, mock_get_instance, mock_sleep, mock_cancel_current_stage
    ) -> None:
        for (
            stage,
            publisher_statuses,
            partner_statuses,
            result,
        ) in self._get_wait_stage_completed():
            with self.subTest(
                stage=stage,
                publisher_statuses=publisher_statuses,
                partner_statuses=partner_statuses,
                result=result,
            ):
                self.mock_graph_api_client.get_instance.side_effect = (
                    self._get_graph_api_output(status) for status in publisher_statuses
                )
                mock_get_instance.side_effect = (
                    self._get_pc_instance(status) for status in partner_statuses
                )

                runner = self._get_runner(type(stage))
                if not result:
                    with self.assertRaises(PLInstanceCalculationException):
                        runner.wait_stage_complete(stage)
                else:
                    runner.wait_stage_complete(stage)

    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.get_instance")
    def test_wait_until_not_timeout(self, mock_get_instance, mock_sleep) -> None:
        stage = PrivateComputationStageFlow.PREPARE
        self.mock_graph_api_client.invoke_operation.call_count = 0
        self.mock_graph_api_client.get_instance.side_effect = (
            self._get_graph_api_output(status)
            for status in (
                "TIMEOUT",
                "TIMEOUT",
                "PROCESSING_REQUEST",
                "PREPARE_DATA_STARTED",
                "PREPARE_DATA_STARTED",
            )
        )

        mock_get_instance.return_value = self._get_pc_instance(
            stage.previous_stage.completed_status
        )

        runner = self._get_runner(PrivateComputationStageFlow)

        self.mock_graph_api_client.invoke_operation.assert_called_with(
            self.instance_id, "NEXT"
        )

        self.assertEqual(stage.started_status, runner.publisher.status)

        # make sure that run next is only being called when the status is timeout
        self.mock_graph_api_client.invoke_operation.call_count = 0
        self.mock_graph_api_client.get_instance.side_effect = (
            self._get_graph_api_output("PREPARE_DATA_COMPLETED") for _ in range(2)
        )

        runner = self._get_runner(PrivateComputationStageFlow)

        self.mock_graph_api_client.invoke_operation.assert_not_called()
        self.assertEqual(stage.completed_status, runner.publisher.status)

    def _get_runner(
        self, stage_flow: Type[PrivateComputationBaseStageFlow]
    ) -> PLInstanceRunner:
        return PLInstanceRunner(
            config={},
            instance_id=self.instance_id,
            input_path="fake_input_path",
            num_shards=self.num_shards,
            logger=self.mock_logger,
            client=self.mock_graph_api_client,
            num_tries=2,
            dry_run=False,
            stage_flow=stage_flow,
        )

    def _get_graph_api_output(self, status: str) -> requests.Response:
        data = {"status": status, "server_ips": ["xxx" for _ in range(self.num_shards)]}
        r = requests.Response()
        r.status_code = 200
        # pyre-ignore
        type(r).text = PropertyMock(return_value=json.dumps(data))

        def json_func(**kwargs) -> Any:
            return data

        r.json = json_func
        return r

    def _get_pc_instance(
        self, status: PrivateComputationInstanceStatus
    ) -> PrivateComputationInstance:
        return PrivateComputationInstance(
            instance_id=self.instance_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=status,
            status_update_ts=1600000000,
            num_pid_containers=self.num_shards,
            num_mpc_containers=self.num_shards,
            num_files_per_mpc_container=40,
            game_type=PrivateComputationGameType.LIFT,
            input_path="456",
            output_dir="789",
        )

    def _get_stage_ready_data(
        self,
    ) -> List[
        Tuple[
            str,
            PrivateComputationInstanceStatus,
            PrivateComputationBaseStageFlow,
            bool,
            bool,
        ]
    ]:
        """
        Tuple represents:
            * publisher status
            * partner status
            * stage
            * is the stage ready for publisher
            * is the stage ready for partner
        """
        return [
            (
                "CREATED",
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                False,
                False,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "RESULT_READY",
                PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                False,
                False,
            ),
            ####################### NON JOINT STAGE TEST #################################3
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationStageFlow.PREPARE.previous_stage.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.previous_stage.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.started_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                True,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.started_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                True,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.failed_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                False,
            ),
            (
                "PREPARE_DATA_FAILED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                False,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
        ]

    def _get_valid_stage_data(
        self,
    ) -> List[
        Tuple[
            str,
            PrivateComputationInstanceStatus,
            Optional[PrivateComputationBaseStageFlow],
            bool,
            bool,
        ]
    ]:
        """
        Tuple represents:
            * publisher status
            * partner status
            * valid stage
            * publisher should invoke
            * partner should invoke
        """
        return [
            (
                "CREATED",
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                False,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                False,
                False,
            ),
            (
                "ID_MATCH_FAILED",
                PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                PrivateComputationLegacyStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                False,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_FAILED",
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                PrivateComputationLegacyStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                False,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                False,
                False,
            ),
            (
                "AGGREGATION_FAILED",
                PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                PrivateComputationLegacyStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "RESULT_READY",
                PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                None,
                False,
                False,
            ),
            ####################### NON JOINT STAGE TEST #################################3
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationStageFlow.PREPARE.previous_stage.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.previous_stage.completed_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.started_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                False,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.started_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                False,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.failed_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                True,
            ),
            (
                "PREPARE_DATA_STARTED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.PREPARE,
                False,
                False,
            ),
            (
                "PREPARE_DATA_FAILED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.PREPARE,
                True,
                False,
            ),
            (
                "PREPARE_DATA_COMPLETED",
                PrivateComputationStageFlow.PREPARE.completed_status,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
        ]

    def _get_wait_stage_start_data(
        self,
    ) -> List[Tuple[PrivateComputationBaseStageFlow, List[str], bool,]]:
        """
        Tuple represents:
            * stage
            * order of the publisher statuses
            * Does it succeed
        """
        return [
            (
                PrivateComputationLegacyStageFlow.ID_MATCH,
                [
                    "CREATED",
                    "CREATED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "ID_MATCH_STARTED",
                    "ID_MATCH_STARTED",
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.ID_MATCH,
                [
                    "CREATED",
                    "CREATED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "ID_MATCH_FAILED",
                    "ID_MATCH_FAILED",
                ],
                False,
            ),
            (
                PrivateComputationLegacyStageFlow.COMPUTE,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "COMPUTATION_STARTED",
                    "COMPUTATION_STARTED",
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.COMPUTE,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "COMPUTATION_FAILED",
                    "COMPUTATION_FAILED",
                ],
                False,
            ),
            (
                PrivateComputationLegacyStageFlow.AGGREGATE,
                [
                    "COMPUTATION_COMPLETED",
                    "COMPUTATION_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "AGGREGATION_STARTED",
                    "AGGREGATION_STARTED",
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.AGGREGATE,
                [
                    "COMPUTATION_COMPLETED",
                    "COMPUTATION_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "AGGREGATION_FAILED",
                    "AGGREGATION_FAILED",
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "PREPARE_DATA_FAILED",
                    "PREPARE_DATA_FAILED",
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "TIMEOUT",
                    "TIMEOUT",
                ],
                False,
            ),
        ]

    def _get_wait_stage_completed(
        self,
    ) -> List[
        Tuple[
            PrivateComputationBaseStageFlow,
            List[str],
            List[PrivateComputationInstanceStatus],
            bool,
        ]
    ]:
        """
        Tuple represents:
            * stage
            * order of the publisher statuses
            * order of the partner statuses
            * Does it succeed
        """
        return [
            (
                PrivateComputationLegacyStageFlow.ID_MATCH,
                [
                    "ID_MATCH_STARTED",
                    "ID_MATCH_STARTED",
                    "ID_MATCH_STARTED",
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                ],
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.ID_MATCH,
                [
                    "ID_MATCH_STARTED",
                    "ID_MATCH_STARTED",
                    "ID_MATCH_STARTED",
                    "ID_MATCH_FAILED",
                    "ID_MATCH_FAILED",
                ],
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationLegacyStageFlow.COMPUTE,
                [
                    "COMPUTATION_STARTED",
                    "COMPUTATION_STARTED",
                    "COMPUTATION_COMPLETED",
                    "COMPUTATION_COMPLETED",
                    "COMPUTATION_COMPLETED",
                ],
                [
                    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                    PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                    PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.COMPUTE,
                [
                    "COMPUTATION_STARTED",
                    "COMPUTATION_STARTED",
                    "COMPUTATION_STARTED",
                    "COMPUTATION_FAILED",
                    "COMPUTATION_FAILED",
                ],
                [
                    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                    PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                    PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                    PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationLegacyStageFlow.AGGREGATE,
                [
                    "AGGREGATION_STARTED",
                    "AGGREGATION_STARTED",
                    "AGGREGATION_STARTED",
                    "RESULT_READY",
                    "RESULT_READY",
                ],
                [
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                    PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                    PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationLegacyStageFlow.AGGREGATE,
                [
                    "AGGREGATION_STARTED",
                    "AGGREGATION_STARTED",
                    "AGGREGATION_STARTED",
                    "AGGREGATION_FAILED",
                    "AGGREGATION_FAILED",
                ],
                [
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                    PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                    PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_COMPLETED",
                    "PREPARE_DATA_COMPLETED",
                ],
                [
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_FAILED",
                    "PREPARE_DATA_FAILED",
                ],
                [
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.PREPARE,
                [
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "PREPARE_DATA_STARTED",
                    "TIMEOUT",
                    "TIMEOUT",
                ],
                [
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
                    PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
                ],
                False,
            ),
        ]
