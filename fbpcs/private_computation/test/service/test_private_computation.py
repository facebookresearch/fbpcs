#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from collections import defaultdict
from typing import List, Optional, Tuple
from unittest.mock import MagicMock, call, patch
from unittest.mock import Mock

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.mpc import MPCInstanceStatus, MPCParty, MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDInstanceStatus,
    PIDRole,
)
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_decoupled_stage_flow import (
    PrivateComputationDecoupledStageFlow,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    UnionedPCInstance,
)
from fbpcs.private_computation.entity.private_computation_legacy_stage_flow import (
    PrivateComputationLegacyStageFlow,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
    NUM_NEW_SHARDS_PER_FILE,
    DEFAULT_K_ANONYMITY_THRESHOLD,
    DEFAULT_PID_PROTOCOL,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    create_and_start_mpc_instance,
    gen_mpc_game_args_to_retry,
    map_private_computation_role_to_mpc_party,
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
)

# TODO T94666166: libfb won't work in OSS
from libfb.py.asyncio.mock import AsyncMock
from libfb.py.testutil import data_provider


def _get_valid_stages_data() -> List[Tuple[PrivateComputationBaseStageFlow]]:
    return [
        (PrivateComputationLegacyStageFlow.ID_MATCH,),
        (PrivateComputationLegacyStageFlow.COMPUTE,),
        (PrivateComputationLegacyStageFlow.AGGREGATE,),
        (PrivateComputationLegacyStageFlow.POST_PROCESSING_HANDLERS,),
        (PrivateComputationDecoupledStageFlow.ID_MATCH,),
        (PrivateComputationDecoupledStageFlow.DECOUPLED_ATTRIBUTION,),
        (PrivateComputationDecoupledStageFlow.DECOUPLED_AGGREGATION,),
        (PrivateComputationDecoupledStageFlow.AGGREGATE,),
    ]


class TestPrivateComputationService(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        container_svc_patcher = patch("fbpcp.service.container_aws.AWSContainerService")
        storage_svc_patcher = patch("fbpcp.service.storage_s3.S3StorageService")
        mpc_instance_repo_patcher = patch(
            "fbpcs.common.repository.mpc_instance_local.LocalMPCInstanceRepository"
        )
        pid_instance_repo_patcher = patch(
            "fbpcs.pid.repository.pid_instance_local.LocalPIDInstanceRepository"
        )
        private_computation_instance_repo_patcher = patch(
            "fbpcs.private_computation.repository.private_computation_instance_local.LocalPrivateComputationInstanceRepository"
        )
        mpc_game_svc_patcher = patch("fbpcp.service.mpc_game.MPCGameService")
        container_svc = container_svc_patcher.start()
        storage_svc = storage_svc_patcher.start()
        mpc_instance_repository = mpc_instance_repo_patcher.start()
        pid_instance_repository = pid_instance_repo_patcher.start()
        private_computation_instance_repository = (
            private_computation_instance_repo_patcher.start()
        )
        mpc_game_svc = mpc_game_svc_patcher.start()

        for patcher in (
            container_svc_patcher,
            storage_svc_patcher,
            mpc_instance_repo_patcher,
            pid_instance_repo_patcher,
            private_computation_instance_repo_patcher,
            mpc_game_svc_patcher,
        ):
            self.addCleanup(patcher.stop)

        self.onedocker_service_config = OneDockerServiceConfig(
            task_definition="test_task_definition",
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/", binary_version="latest"
            )
        )

        self.onedocker_service = OneDockerService(
            container_svc, self.onedocker_service_config.task_definition
        )

        self.mpc_service = MPCService(
            container_svc=container_svc,
            instance_repository=mpc_instance_repository,
            task_definition="test_task_definition",
            mpc_game_svc=mpc_game_svc,
        )

        self.pid_service = PIDService(
            instance_repository=pid_instance_repository,
            storage_svc=storage_svc,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
        )

        self.private_computation_service = PrivateComputationService(
            instance_repository=private_computation_instance_repository,
            storage_svc=storage_svc,
            mpc_svc=self.mpc_service,
            pid_svc=self.pid_service,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            pid_config={},
        )

        self.test_private_computation_id = "test_private_computation_id"
        self.test_num_containers = 2
        self.test_input_path = "in_path"
        self.test_output_dir = "out_dir"
        self.test_game_type = PrivateComputationGameType.LIFT
        self.test_concurrency = 1
        self.test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

    def test_create_instance(self):
        test_role = PrivateComputationRole.PUBLISHER
        self.private_computation_service.create_instance(
            instance_id=self.test_private_computation_id,
            role=test_role,
            game_type=self.test_game_type,
            input_path=self.test_input_path,
            output_dir=self.test_output_dir,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            concurrency=self.test_concurrency,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            hmac_key=self.test_hmac_key,
        )
        # check instance_repository.create is called with the correct arguments
        self.private_computation_service.instance_repository.create.assert_called()
        args = self.private_computation_service.instance_repository.create.call_args[0][
            0
        ]
        self.assertEqual(self.test_private_computation_id, args.instance_id)
        self.assertEqual(test_role, args.role)
        self.assertEqual(PrivateComputationInstanceStatus.CREATED, args.status)

    def test_update_instance(self):
        test_pid_id = self.test_private_computation_id + "_id_match"
        test_pid_role = PIDRole.PUBLISHER
        test_input_path = "pid_in"
        test_output_path = "pid_out"
        # create one PID instance to be put into PrivateComputationInstance
        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=DEFAULT_PID_PROTOCOL,
            pid_role=test_pid_role,
            num_shards=self.test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
            status=PIDInstanceStatus.STARTED,
        )

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            instances=[pid_instance],
        )

        updated_pid_instance = pid_instance
        updated_pid_instance.status = PIDInstanceStatus.COMPLETED
        self.private_computation_service.pid_svc.update_instance = MagicMock(
            return_value=updated_pid_instance
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # call update on the PrivateComputationInstance
        updated_instance = self.private_computation_service.update_instance(
            instance_id=self.test_private_computation_id
        )

        # check update instance called on the right pid instance
        self.private_computation_service.pid_svc.update_instance.assert_called()
        self.assertEqual(
            test_pid_id,
            self.private_computation_service.pid_svc.update_instance.call_args[0][0],
        )

        # check update instance called on the right private lift instance
        self.private_computation_service.instance_repository.update.assert_called()
        self.assertEqual(
            private_computation_instance,
            self.private_computation_service.instance_repository.update.call_args[0][0],
        )

        # check updated_instance has new status
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            updated_instance.status,
        )

        # create one MPC instance to be put into PrivateComputationInstance
        test_mpc_id = "test_mpc_id"
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=GameNames.LIFT.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
        )

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            instances=[mpc_instance],
        )

        updated_mpc_instance = mpc_instance
        updated_mpc_instance.status = MPCInstanceStatus.COMPLETED
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=updated_mpc_instance
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        # call update on the PrivateComputationInstance
        updated_instance = self.private_computation_service.update_instance(
            instance_id=self.test_private_computation_id
        )

        # check update instance called on the right mpc instance
        self.private_computation_service.mpc_svc.update_instance.assert_called()
        self.assertEqual(
            test_mpc_id,
            self.private_computation_service.mpc_svc.update_instance.call_args[0][0],
        )

        # check update instance called on the right private lift instance
        self.private_computation_service.instance_repository.update.assert_called()
        self.assertEqual(
            private_computation_instance,
            self.private_computation_service.instance_repository.update.call_args[0][0],
        )

        # check updated_instance has new status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            updated_instance.status,
        )

    @staticmethod
    def _get_dummy_stage_svc() -> PrivateComputationStageService:
        """create a DummyTestStageService class and instantiate an instance of it"""

        return type(
            "DummyTestStageService",
            (PrivateComputationStageService,),
            {
                "run_async": AsyncMock(
                    # run_async will return whatever pc_instance privatelift.run_stage passes it
                    side_effect=lambda pc_instance, *args, **kwargs: pc_instance
                ),
                "get_status": Mock(
                    # run_async will return whatever pc_instance privatelift.run_stage passes it
                    side_effect=lambda pc_instance, *args, **kwargs: pc_instance.status
                ),
            },
        )()

    def test_get_next_runnable_stage_completed_status(self) -> None:
        flow = PrivateComputationLegacyStageFlow
        status = PrivateComputationInstanceStatus.CREATED

        instance = self.create_sample_instance(status)
        instance._stage_flow_cls_name = flow.get_cls_name()

        self.assertEqual(flow.ID_MATCH, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_failed_status(self) -> None:
        flow = PrivateComputationLegacyStageFlow
        status = PrivateComputationInstanceStatus.ID_MATCHING_FAILED

        instance = self.create_sample_instance(status)
        instance._stage_flow_cls_name = flow.get_cls_name()

        self.assertEqual(flow.ID_MATCH, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_started_status(self) -> None:
        flow = PrivateComputationLegacyStageFlow
        status = PrivateComputationInstanceStatus.ID_MATCHING_STARTED

        instance = self.create_sample_instance(status)
        instance._stage_flow_cls_name = flow.get_cls_name()

        self.assertEqual(None, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_nothing_left(self) -> None:
        flow = PrivateComputationLegacyStageFlow
        status = PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED

        instance = self.create_sample_instance(status)
        instance._stage_flow_cls_name = flow.get_cls_name()

        self.assertEqual(None, instance.get_next_runnable_stage())

    @data_provider(_get_valid_stages_data)
    def test_run_stage_correct_stage_order(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        tests that run_stage runs stage_svc when the stage_svc is the next stage in the sequence
        """
        ################# PREVIOUS STAGE COMPLETED OR RETRY #######################
        stage_svc = self._get_dummy_stage_svc()
        for status in (
            stage.previous_stage.completed_status,
            stage.failed_status,
        ):
            pl_instance = self.create_sample_instance(status=status)
            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            pl_instance = self.private_computation_service.run_stage(
                pl_instance.instance_id, stage, stage_svc
            )
            self.assertEqual(pl_instance.status, stage.started_status)

    @data_provider(_get_valid_stages_data)
    def test_run_stage_status_already_started(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        tests that run_stage does not run stage_svc when the instance status is already started
        """
        ################# CURRENT STAGE STATUS NOT VALID #######################
        stage_svc = self._get_dummy_stage_svc()
        pl_instance = self.create_sample_instance(status=stage.started_status)

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=pl_instance
        )

        with self.assertRaises(ValueError):
            pl_instance = self.private_computation_service.run_stage(
                pl_instance.instance_id, stage, stage_svc
            )

    @data_provider(_get_valid_stages_data)
    def test_run_stage_out_of_order_with_dry_run(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        tests that run_stage runs stage_svc out of order when dry run is passed
        """
        ################ STAGE OUT OF ORDER WITH DRY RUN #####################
        stage_svc = self._get_dummy_stage_svc()
        pl_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.UNKNOWN
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=pl_instance
        )

        pl_instance = self.private_computation_service.run_stage(
            pl_instance.instance_id, stage, stage_svc, dry_run=True
        )
        self.assertEqual(pl_instance.status, stage.started_status)

    @data_provider(_get_valid_stages_data)
    def test_run_stage_out_of_order_without_dry_run(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        tests that run_stage does not run stage_svc out of order when dry run is not passed
        """
        ####################### STAGE OUT OF ORDER NO DRY RUN ############################
        stage_svc = self._get_dummy_stage_svc()
        pl_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.UNKNOWN
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=pl_instance
        )

        with self.assertRaises(ValueError):
            pl_instance = self.private_computation_service.run_stage(
                pl_instance.instance_id, stage, stage_svc, dry_run=False
            )

    @data_provider(_get_valid_stages_data)
    def test_run_stage_partner_no_server_ips(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        if it's a joint stage (partner requires server ips) but partner doesn't provide server ips, value error is thrown.
        Otherwise, things run as they should.
        """
        ####################### PARTNER NO SERVER IPS ############################
        stage_svc = self._get_dummy_stage_svc()
        pl_instance = self.create_sample_instance(
            status=stage.previous_stage.completed_status,
            role=PrivateComputationRole.PARTNER,
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=pl_instance
        )

        if stage.is_joint_stage:
            with self.assertRaises(ValueError):
                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.instance_id, stage, stage_svc
                )
        else:
            pl_instance = self.private_computation_service.run_stage(
                pl_instance.instance_id, stage, stage_svc
            )
            self.assertEqual(pl_instance.status, stage.started_status)

    @data_provider(_get_valid_stages_data)
    def test_run_stage_fails(
        self,
        stage: PrivateComputationLegacyStageFlow,
    ) -> None:
        """
        tests that statuses are set properly when a run fails
        """
        ######################### STAGE FAILS ####################################
        stage_svc = self._get_dummy_stage_svc()
        pl_instance = self.create_sample_instance(
            status=stage.previous_stage.completed_status
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=pl_instance
        )

        # create a custom exception class to make sure we have a unique exception for the test
        stage_failure_exception = type("TestStageFailureException", (Exception,), {})
        stage_svc.run_async = AsyncMock(side_effect=stage_failure_exception())

        with self.assertRaises(stage_failure_exception):
            pl_instance = self.private_computation_service.run_stage(
                pl_instance.instance_id, stage, stage_svc
            )

        self.assertEqual(pl_instance.status, stage.failed_status)

    @patch("fbpcp.service.mpc.MPCService")
    async def test_create_and_start_mpc_instance(self, mock_mpc_svc):
        mock_mpc_svc.create_instance = MagicMock()
        mock_mpc_svc.start_instance_async = AsyncMock()

        instance_id = "test_instance_id"
        game_name = GameNames.LIFT.value
        mpc_party = MPCParty.CLIENT
        num_containers = 4
        input_file = "input_file"
        output_file = "output_file"
        input_directory = "input_directory"
        output_directory = "output_directory"
        server_ips = ["192.0.2.0", "192.0.2.1"]
        game_args = {
            "input_filenames": input_file,
            "input_directory": input_directory,
            "output_filenames": output_file,
            "output_directory": output_directory,
            "concurrency": 1,
        }
        binary_version = self.onedocker_binary_config_map[
            OneDockerBinaryNames.LIFT_COMPUTE.value
        ].binary_version

        await create_and_start_mpc_instance(
            mpc_svc=mock_mpc_svc,
            instance_id=instance_id,
            game_name=game_name,
            mpc_party=mpc_party,
            num_containers=num_containers,
            binary_version=binary_version,
            container_timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
            server_ips=server_ips,
            game_args=game_args,
        )

        # check create_instance and start_instance were called with the right parameters
        self.assertEqual(
            call(
                instance_id=instance_id,
                game_name=game_name,
                mpc_party=mpc_party,
                num_workers=num_containers,
                game_args=game_args,
            ),
            mock_mpc_svc.create_instance.call_args,
        )

        self.assertEqual(
            call(
                instance_id=instance_id,
                server_ips=server_ips,
                timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
                version=binary_version,
            ),
            mock_mpc_svc.start_instance_async.call_args,
        )

    def test_map_private_computation_role_to_mpc_party(self):
        self.assertEqual(
            MPCParty.SERVER,
            map_private_computation_role_to_mpc_party(PrivateComputationRole.PUBLISHER),
        )
        self.assertEqual(
            MPCParty.CLIENT,
            map_private_computation_role_to_mpc_party(PrivateComputationRole.PARTNER),
        )

    def test_get_status_from_stage(self):
        # Test get status from an MPC stage
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id="test_mpc_id",
            game_name=GameNames.SHARD_AGGREGATOR.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
        )
        pc_instance = self.create_sample_instance(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            instances=[mpc_instance],
        )
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=mpc_instance
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            self.private_computation_service._update_instance(pc_instance).status,
        )

        # Test get status from the PID stage
        pid_instance = PIDInstance(
            instance_id="test_pid_id",
            protocol=DEFAULT_PID_PROTOCOL,
            pid_role=PIDRole.PUBLISHER,
            num_shards=4,
            input_path="input",
            output_path="output",
            stages_containers={},
            stages_status={},
            status=PIDInstanceStatus.COMPLETED,
        )
        pc_instance = self.create_sample_instance(
            PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            instances=[pid_instance],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            self.private_computation_service._update_instance(pc_instance).status,
        )

    def test_validate_metrics_results_doesnt_match(self):
        self.private_computation_service.storage_svc.read = MagicMock()
        self.private_computation_service.storage_svc.read.side_effect = [
            '{"subGroupMetrics":[],"metrics":{"controlClicks":1,"testSpend":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
            '{"subGroupMetrics":[],"metrics":{"testSpend":0,"controlClicks":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
        ]
        with self.assertRaises(PrivateComputationServiceValidationError):
            self.private_computation_service.validate_metrics(
                instance_id="test_id",
                aggregated_result_path="aggregated_result_path",
                expected_result_path="expected_result_path",
            )

    def test_cancel_current_stage(self):
        test_mpc_id = self.test_private_computation_id + "_compute_metrics"
        test_game_name = GameNames.LIFT.value
        test_mpc_party = MPCParty.CLIENT

        # prepare the pl instance that will be read in to memory from the repository
        # at the beginning of the cancel_current_stage function
        mpc_instance_started = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.STARTED,
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance_started],
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # prepare the mpc instance that's returned from mpc_service.stop_instance()
        mpc_instance_canceled = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.CANCELED,
        )
        self.private_computation_service.mpc_svc.stop_instance = MagicMock(
            return_value=mpc_instance_canceled
        )
        self.private_computation_service.mpc_svc.instance_repository.read = MagicMock(
            return_value=mpc_instance_canceled
        )

        # call cancel, expect no exception
        private_computation_instance = (
            self.private_computation_service.cancel_current_stage(
                instance_id=self.test_private_computation_id,
            )
        )

        # assert the pl instance returned has the correct status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            private_computation_instance.status,
        )

    def test_gen_game_args_to_retry(self):
        test_input = "test_input_retry"
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id="mpc_instance",
            game_name=GameNames.LIFT.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
            containers=[
                ContainerInstance(
                    instance_id="container_instance_0",
                    status=ContainerInstanceStatus.FAILED,
                ),
                ContainerInstance(
                    instance_id="container_instance_1",
                    status=ContainerInstanceStatus.COMPLETED,
                ),
            ],
            game_args=[
                {
                    "input_filenames": test_input,
                },
                {
                    "input_filenames": "input_filenames",
                },
            ],
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            instances=[mpc_instance],
        )

        game_args = gen_mpc_game_args_to_retry(private_computation_instance)

        self.assertEqual(1, len(game_args))  # only 1 failed container
        self.assertEqual(test_input, game_args[0]["input_filenames"])

    def create_sample_instance(
        self,
        status: PrivateComputationInstanceStatus,
        role: PrivateComputationRole = PrivateComputationRole.PUBLISHER,
        instances: Optional[List[UnionedPCInstance]] = None,
    ) -> PrivateComputationInstance:
        return PrivateComputationInstance(
            instance_id=self.test_private_computation_id,
            role=role,
            instances=instances or [],
            status=status,
            status_update_ts=1600000000,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            concurrency=self.test_concurrency,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            game_type=PrivateComputationGameType.LIFT,
            input_path=self.test_input_path,
            output_dir=self.test_output_dir,
            fail_fast=True,
            k_anonymity_threshold=DEFAULT_K_ANONYMITY_THRESHOLD,
            hmac_key=self.test_hmac_key,
        )
