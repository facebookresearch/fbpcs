#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
from typing import Any, List, Optional, Tuple, Type
from unittest import TestCase
from unittest.mock import patch, PropertyMock

import requests
from fbpcs.pl_coordinator.pc_graphapi_utils import GRAPHAPI_INSTANCE_STATUSES
from fbpcs.pl_coordinator.pl_instance_runner import (
    IncompatibleStageError,
    PCInstanceCalculationException,
    PLInstanceRunner,
)
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)


class TestPlInstanceRunner(TestCase):
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
        self.num_tries = 2
        self.instance_id = "123"

    @patch(
        "fbpcs.pl_coordinator.pc_calc_instance.PrivateComputationCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
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
        "fbpcs.pl_coordinator.pc_calc_instance.PrivateComputationCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
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
                    PrivateComputationStageFlow if not stage else type(stage)
                )
                runner.publisher.status = GRAPHAPI_INSTANCE_STATUSES[publisher_status]
                runner.partner.status = partner_status
                valid_stage = runner.get_valid_stage()
                self.assertEqual(valid_stage, stage)
        for publisher_status, partner_status in (
            ("PID_PREPARE_COMPLETED", PrivateComputationInstanceStatus.CREATED),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.RESHARD_COMPLETED,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            ),
            ("CREATED", PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED),
            (
                "RESHARD_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            ),
            (
                "COMPUTATION_FAILED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            ),
        ):
            with self.subTest(
                "Testing incompatible stages",
                publisher_status=publisher_status,
                partner_status=partner_status,
            ):
                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)

                runner = self._get_runner(PrivateComputationStageFlow)
                runner.publisher.status = GRAPHAPI_INSTANCE_STATUSES[publisher_status]
                runner.partner.status = partner_status
                with self.assertRaises(IncompatibleStageError):
                    stage = runner.get_valid_stage()

    @patch(
        "fbpcs.pl_coordinator.pc_calc_instance.PrivateComputationCalcInstance.wait_valid_status"
    )
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
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

    @patch("fbpcs.pl_coordinator.pc_partner_instance.run_stage")
    @patch(
        "fbpcs.pl_coordinator.pc_calc_instance.PrivateComputationCalcInstance.wait_stage_start"
    )
    @patch(
        "fbpcs.pl_coordinator.pl_instance_runner.PLInstanceRunner.wait_stage_complete"
    )
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
    def test_run_stage(
        self,
        mock_get_instance,
        mock_wait_stage_complete,
        mock_wait_stage_start,
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

    @patch("fbpcs.pl_coordinator.pc_calc_instance.sleep")
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
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
                    # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                    stage.previous_stage.completed_status
                )

                runner = self._get_runner(type(stage))
                if not result:
                    with self.assertRaises(PCInstanceCalculationException):
                        runner.publisher.wait_stage_start(stage)
                else:
                    runner.publisher.wait_stage_start(stage)

    @patch("fbpcs.pl_coordinator.pc_partner_instance.cancel_current_stage")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
    def test_wait_stage_completed(
        self, mock_get_instance, mock_sleep, mock_cancel_current_stage
    ) -> None:
        for (
            stage,
            publisher_statuses,
            partner_statuses,
            result,
        ) in self._get_wait_stage_completed():
            mock_cancel_current_stage.reset_mock()
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
                    with self.assertRaises(PCInstanceCalculationException):
                        runner.wait_stage_complete(stage)

                    if stage.is_joint_stage:
                        # make sure when any of role fail it will try to call cancel_current_stage
                        mock_cancel_current_stage.assert_called_once_with(
                            config={},
                            instance_id=self.instance_id,
                            logger=self.mock_logger,
                        )
                    else:
                        mock_cancel_current_stage.assert_not_called()
                else:
                    runner.wait_stage_complete(stage)
                    mock_cancel_current_stage.assert_not_called()

    @patch("fbpcs.pl_coordinator.pc_calc_instance.sleep")
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
    def test_wait_until_not_timeout(self, mock_get_instance, mock_sleep) -> None:
        stage = PrivateComputationStageFlow.ID_SPINE_COMBINER
        self.mock_graph_api_client.invoke_operation.call_count = 0
        self.mock_graph_api_client.get_instance.side_effect = (
            self._get_graph_api_output(status)
            for status in (
                "TIMEOUT",
                "TIMEOUT",
                "PROCESSING_REQUEST",
                "ID_SPINE_COMBINER_STARTED",
                "ID_SPINE_COMBINER_STARTED",
            )
        )

        mock_get_instance.return_value = self._get_pc_instance(
            # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
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
            self._get_graph_api_output("ID_SPINE_COMBINER_COMPLETED") for _ in range(2)
        )

        runner = self._get_runner(PrivateComputationStageFlow)

        self.mock_graph_api_client.invoke_operation.assert_not_called()
        self.assertEqual(stage.completed_status, runner.publisher.status)

    @patch("fbpcs.pl_coordinator.pl_instance_runner.PLInstanceRunner.run_stage")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
    def test_run_retries(self, mock_get_instance, mock_sleep, mock_run_stage) -> None:

        subtest_data = [
            (
                "ID_MATCH_STARTED",
                PrivateComputationStageFlow.ID_MATCH.started_status,
                PrivateComputationStageFlow.ID_MATCH.is_retryable,
            ),
            (
                "CREATED",
                PrivateComputationStageFlow.PC_PRE_VALIDATION.started_status,
                PrivateComputationStageFlow.PC_PRE_VALIDATION.is_retryable,
            ),
        ]
        for (
            publisher_status,
            partner_status,
            is_retryable,
        ) in subtest_data:
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                is_retryable=is_retryable,
            ):
                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                mock_get_instance.return_value = self._get_pc_instance(partner_status)
                mock_run_stage.call_count = 0
                mock_run_stage.side_effect = PCInstanceCalculationException(
                    "force eception", "", ""
                )

                runner = self._get_runner(PrivateComputationStageFlow)
                with self.assertRaises(PCInstanceCalculationException):
                    runner.run()

                if is_retryable:
                    self.assertEqual(mock_run_stage.call_count, self.num_tries)
                else:
                    self.assertEqual(mock_run_stage.call_count, 1)

    @patch("fbpcs.pl_coordinator.pc_partner_instance.update_input_path")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.PLInstanceRunner.run_stage")
    @patch("fbpcs.pl_coordinator.pl_instance_runner.sleep")
    @patch("fbpcs.pl_coordinator.pc_partner_instance.get_instance")
    def test_partner_input_overwrite(
        self, mock_get_instance, mock_sleep, mock_run_stage, mock_update_input_path
    ) -> None:

        subtest_data = [
            (
                "CREATED",
                PrivateComputationStageFlow.CREATED.completed_status,
                True,
            ),
            (
                "CREATED",
                PrivateComputationStageFlow.PC_PRE_VALIDATION.failed_status,
                True,
            ),
            (
                "CREATED",
                PrivateComputationStageFlow.ID_MATCH.started_status,
                False,
            ),
        ]
        for (
            publisher_status,
            partner_status,
            need_override,
        ) in subtest_data:
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                need_override=need_override,
            ):
                self.mock_graph_api_client.get_instance.return_value = (
                    self._get_graph_api_output(publisher_status)
                )
                old_pc_instance = self._get_pc_instance(partner_status)
                old_pc_instance.product_config.common.input_path = "need_to_be_updated"
                mock_get_instance.return_value = old_pc_instance
                if need_override:
                    self._get_runner(PrivateComputationStageFlow)
                    mock_update_input_path.assert_called_with(
                        {}, "123", "fake_input_path", self.mock_logger
                    )
                else:
                    # should have exception raised to warn partner
                    with self.assertRaises(PCInstanceCalculationException):
                        self._get_runner(PrivateComputationStageFlow)

    def _get_runner(
        self, stage_flow: Type[PrivateComputationBaseStageFlow]
    ) -> PLInstanceRunner:
        return PLInstanceRunner(
            config={},
            instance_id=self.instance_id,
            input_path="fake_input_path",
            game_type=PrivateComputationGameType.LIFT,
            attribution_rule=AttributionRule.LAST_CLICK_1D,
            aggregation_type=AggregationType.MEASUREMENT,
            num_mpc_containers=self.num_shards,
            num_pid_containers=self.num_shards,
            logger=self.mock_logger,
            client=self.mock_graph_api_client,
            num_tries=self.num_tries,
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
        infra_config: InfraConfig = InfraConfig(
            self.instance_id,
            PrivateComputationRole.PARTNER,
            status,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=self.num_shards,
            num_mpc_containers=self.num_shards,
            num_files_per_mpc_container=40,
            status_updates=[],
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path="fake_input_path",
            output_dir="789",
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
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
                PrivateComputationStageFlow.CREATED.next_stage,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_MATCH.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                PrivateComputationStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationStageFlow.ID_MATCH,
                False,
                False,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationStageFlow.ID_MATCH.next_stage,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.COMPUTE.previous_stage.completed_status,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.AGGREGATE.previous_stage.completed_status,
                PrivateComputationStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                PrivateComputationStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationStageFlow.AGGREGATE,
                True,
                True,
            ),
            (
                "RESULT_READY",
                PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                PrivateComputationStageFlow.AGGREGATE,
                False,
                False,
            ),
            ####################### NON JOINT STAGE TEST #################################3
            (
                "ID_MATCHING_POST_PROCESS_COMPLETED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                True,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                True,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.failed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                False,
            ),
            (
                "ID_SPINE_COMBINER_FAILED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                False,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.RESHARD,
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
                PrivateComputationStageFlow.CREATED.next_stage,
                True,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_MATCH.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_MATCH,
                False,
                True,
            ),
            (
                "ID_MATCH_STARTED",
                PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PrivateComputationStageFlow.ID_MATCH,
                False,
                False,
            ),
            (
                "ID_MATCH_FAILED",
                PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                PrivateComputationStageFlow.ID_MATCH,
                True,
                True,
            ),
            (
                "ID_MATCH_COMPLETED",
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationStageFlow.ID_MATCH.next_stage,
                True,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.COMPUTE.previous_stage.completed_status,
                PrivateComputationStageFlow.COMPUTE,
                False,
                True,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_FAILED",
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                PrivateComputationStageFlow.COMPUTE,
                True,
                True,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                PrivateComputationStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_STARTED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationStageFlow.COMPUTE,
                False,
                False,
            ),
            (
                "COMPUTATION_COMPLETED",
                PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                True,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.AGGREGATE.previous_stage.completed_status,
                PrivateComputationStageFlow.AGGREGATE,
                False,
                True,
            ),
            (
                "AGGREGATION_STARTED",
                PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                PrivateComputationStageFlow.AGGREGATE,
                False,
                False,
            ),
            (
                "AGGREGATION_FAILED",
                PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                PrivateComputationStageFlow.AGGREGATE,
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
                "ID_MATCHING_POST_PROCESS_COMPLETED",
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                False,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                False,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.failed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                True,
            ),
            (
                "ID_SPINE_COMBINER_STARTED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                False,
                False,
            ),
            (
                "ID_SPINE_COMBINER_FAILED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                True,
                False,
            ),
            (
                "ID_SPINE_COMBINER_COMPLETED",
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.RESHARD,
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
                PrivateComputationStageFlow.ID_MATCH,
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
                PrivateComputationStageFlow.ID_MATCH,
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
                PrivateComputationStageFlow.COMPUTE,
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
                PrivateComputationStageFlow.COMPUTE,
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
                PrivateComputationStageFlow.AGGREGATE,
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
                PrivateComputationStageFlow.AGGREGATE,
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
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                [
                    "ID_MATCH_COMPLETED",
                    "ID_MATCH_COMPLETED",
                    "PROCESSING_REQUEST",
                    "PROCESSING_REQUEST",
                    "ID_SPINE_COMBINER_FAILED",
                    "ID_SPINE_COMBINER_FAILED",
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
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
                PrivateComputationStageFlow.ID_MATCH,
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
                PrivateComputationStageFlow.ID_MATCH,
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
                PrivateComputationStageFlow.COMPUTE,
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
                PrivateComputationStageFlow.COMPUTE,
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
                PrivateComputationStageFlow.AGGREGATE,
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
                PrivateComputationStageFlow.AGGREGATE,
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
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                [
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_COMPLETED",
                    "ID_SPINE_COMBINER_COMPLETED",
                ],
                [
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                [
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_FAILED",
                    "ID_SPINE_COMBINER_FAILED",
                ],
                [
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                [
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "ID_SPINE_COMBINER_STARTED",
                    "TIMEOUT",
                    "TIMEOUT",
                ],
                [
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
                    PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
                ],
                False,
            ),
        ]
