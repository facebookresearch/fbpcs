#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from collections import defaultdict
from typing import List, Optional
from unittest.mock import MagicMock, call, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.mpc import MPCInstanceStatus, MPCParty, MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.data_processing.lift_id_combiner.lift_id_spine_combiner_cpp import (
    CppLiftIdSpineCombinerService,
)
from fbpcs.data_processing.sharding.sharding_cpp import CppShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.pcf.tests.async_utils import to_sync
from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDProtocol,
    PIDRole,
    PIDInstanceStatus,
)
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    UnionedPCInstance,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    NUM_NEW_SHARDS_PER_FILE,
    DEFAULT_K_ANONYMITY_THRESHOLD,
)

# TODO T94666166: libfb won't work in OSS
from libfb.py.asyncio.mock import AsyncMock


class TestPrivateComputationService(unittest.TestCase):
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
            storage_svc=storage_svc,
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
            mpc_svc=self.mpc_service,
            pid_svc=self.pid_service,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
        )

        self.test_private_computation_id = "test_private_computation_id"
        self.test_num_containers = 2
        self.test_input_path = "in_path"
        self.test_output_dir = "out_dir"
        self.test_game_type = PrivateComputationGameType.LIFT
        self.test_concurrency = 1

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
        test_pid_protocol = PIDProtocol.UNION_PID
        test_pid_role = PIDRole.PUBLISHER
        test_input_path = "pid_in"
        test_output_path = "pid_out"
        # create one PID instance to be put into PrivateComputationInstance
        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=test_pid_protocol,
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

    def test_id_match(self):
        test_pid_id = self.test_private_computation_id + "_id_match"
        test_pid_protocol = PIDProtocol.UNION_PID
        test_pid_role = PIDRole.PUBLISHER
        test_pid_config = {"key": "value"}
        test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.CREATED
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=test_pid_protocol,
            pid_role=test_pid_role,
            num_shards=self.test_num_containers,
            input_path=private_computation_instance.input_path,
            output_path=private_computation_instance.pid_stage_output_data_path,
        )
        self.pid_service.create_instance = MagicMock(return_value=pid_instance)
        self.pid_service.run_instance = AsyncMock()
        pid_instance.status = PIDInstanceStatus.STARTED
        self.pid_service.get_instance = MagicMock(return_value=pid_instance)

        # call id_match
        self.private_computation_service.id_match(
            instance_id=self.test_private_computation_id,
            protocol=test_pid_protocol,
            pid_config=test_pid_config,
            server_ips=["192.0.2.0", "192.0.2.1"],
            hmac_key=test_hmac_key,
        )

        self.assertEqual(
            test_pid_id,
            self.pid_service.create_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            test_pid_protocol,
            self.pid_service.create_instance.call_args[1]["protocol"],
        )
        self.assertEqual(
            self.test_num_containers,
            self.pid_service.create_instance.call_args[1]["num_shards"],
        )
        self.assertEqual(
            test_pid_role,
            self.pid_service.create_instance.call_args[1]["pid_role"],
        )
        self.assertEqual(
            private_computation_instance.input_path,
            self.pid_service.create_instance.call_args[1]["input_path"],
        )
        self.assertEqual(
            private_computation_instance.pid_stage_output_base_path,
            self.pid_service.create_instance.call_args[1]["output_path"],
        )
        self.assertEqual(
            test_hmac_key,
            self.pid_service.create_instance.call_args[1]["hmac_key"],
        )

        self.assertEqual(
            test_pid_id,
            self.pid_service.run_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            test_pid_config,
            self.pid_service.run_instance.call_args[1]["pid_config"],
        )

        self.private_computation_service.instance_repository.update.assert_called()

        self.assertEqual(pid_instance, private_computation_instance.instances[0])

    def test_id_match_rerun(self):
        # construct a private_computation_instance and a pid_instance
        test_pid_id = self.test_private_computation_id + "_id_match1"
        test_pid_protocol = PIDProtocol.UNION_PID
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        )
        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=test_pid_protocol,
            pid_role=PIDRole.PUBLISHER,
            num_shards=self.test_num_containers,
            input_path=private_computation_instance.input_path,
            output_path=private_computation_instance.pid_stage_output_base_path,
            status=PIDInstanceStatus.STARTED,
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        self.pid_service.create_instance = MagicMock(return_value=pid_instance)
        self.pid_service.run_instance = AsyncMock()
        self.pid_service.get_instance = MagicMock(return_value=pid_instance)

        # call id_match
        self.private_computation_service.id_match(
            instance_id=self.test_private_computation_id,
            protocol=test_pid_protocol,
            pid_config={"key": "value"},
        )

        # check that the retry counter has been incremented
        self.assertEqual(private_computation_instance.retry_counter, 1)

        self.assertEqual(pid_instance, private_computation_instance.instances[0])
        self.assertEqual(
            test_pid_id,
            self.pid_service.create_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            test_pid_id,
            self.pid_service.run_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            private_computation_instance.status,
        )

    def test_id_match_fail(self):
        # construct a private_computation_instance with the status AGGREGATION_COMPLETED
        test_private_computation_id = "test_private_computation_id"
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # expect an exception to be raised due to not passing status check
        with self.assertRaises(ValueError):
            self.private_computation_service.id_match(
                instance_id=test_private_computation_id,
                protocol=PIDProtocol.UNION_PID,
                pid_config={"key": "value"},
            )

    def test_id_match_rerun_fail(self):
        # construct a private_computation_instance with the status ID_MATCHING_COMPLETED
        test_private_computation_id = "test_private_computation_id"
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # expect an exception to be raised because rerun is only allowed on ID_MATCHING_FAILED
        with self.assertRaises(ValueError):
            self.private_computation_service.id_match(
                instance_id=test_private_computation_id,
                protocol=PIDProtocol.UNION_PID,
                pid_config={"key": "value"},
            )

    def test_compute_metrics(self):
        test_private_computation_id = "test_private_computation_id"
        test_mpc_id = test_private_computation_id + "_compute_metrics"
        test_game_name = GameNames.LIFT.value
        test_num_containers = 2
        test_mpc_party = MPCParty.CLIENT
        test_concurrency = 2
        test_server_ips = ["192.0.2.0", "192.0.2.1"]

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            role=PrivateComputationRole.PARTNER,
        )

        test_game_args = [
            {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.compute_stage_output_base_path,
                "file_start_index": 0,
                "num_files": NUM_NEW_SHARDS_PER_FILE,
                "concurrency": test_concurrency,
            },
            {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.compute_stage_output_base_path,
                "file_start_index": NUM_NEW_SHARDS_PER_FILE * 1,
                "num_files": NUM_NEW_SHARDS_PER_FILE,
                "concurrency": test_concurrency,
            },
        ]

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # construct an MPC instance as the mocked object returned by _create_and_start_mpc_instance
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=test_num_containers,
        )
        self.private_computation_service._create_and_start_mpc_instance = AsyncMock(
            return_value=mpc_instance
        )

        # call compute_metrics
        self.private_computation_service.compute_metrics(
            instance_id=test_private_computation_id,
            concurrency=test_concurrency,
            server_ips=test_server_ips,
        )

        self.assertEqual(
            test_mpc_id,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["instance_id"],
        )
        self.assertEqual(
            test_game_name,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_name"],
        )
        self.assertEqual(
            test_num_containers,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["num_containers"],
        )
        self.assertEqual(
            test_mpc_party,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["mpc_party"],
        )
        self.assertEqual(
            test_server_ips,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["server_ips"],
        )
        self.assertEqual(
            test_game_args,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_args"],
        )

        self.private_computation_service.instance_repository.update.assert_called()
        self.assertEqual(mpc_instance, private_computation_instance.instances[0])
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            private_computation_instance.status,
        )

    def test_compute_metrics_rerun(self):
        # construct a private_computation_instance
        test_mpc_id = self.test_private_computation_id + "_compute_metrics"
        test_game_name = GameNames.LIFT.value

        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=MPCParty.CLIENT,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.FAILED,
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            instances=[mpc_instance],
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=mpc_instance
        )

        self.private_computation_service._create_and_start_mpc_instance = AsyncMock()

        # call compute_metrics
        self.private_computation_service.compute_metrics(
            instance_id=self.test_private_computation_id,
            concurrency=2,
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        # check that the retry counter has been incremented
        self.assertEqual(private_computation_instance.retry_counter, 1)

        # check a new MPC instance handling metrics computation was to be created
        self.assertEqual(2, len(private_computation_instance.instances))
        self.assertEqual(
            self.test_private_computation_id + "_compute_metrics1",
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            private_computation_instance.status,
        )

    def test_partner_missing_server_ips(self):
        test_private_computation_id = "test_private_computation_id"
        test_concurrency = 2

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # exception because role is partner but server ips are not given
        with self.assertRaises(ValueError):
            self.private_computation_service.compute_metrics(
                instance_id=test_private_computation_id,
                concurrency=test_concurrency,
            )

        # exception because role is partner but server ips are not given
        with self.assertRaises(ValueError):
            self.private_computation_service.aggregate_shards(
                instance_id=test_private_computation_id,
            )

    def test_aggregate_shards(self):
        # construct a private_computation_instance with an mpc_instance handling metrics computation
        test_mpc_id = self.test_private_computation_id + "_compute_metrics"
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=GameNames.LIFT.value,
            mpc_party=MPCParty.SERVER,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.COMPLETED,
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            instances=[mpc_instance],
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=mpc_instance
        )

        self.private_computation_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_shards
        self.private_computation_service.aggregate_shards(
            instance_id=self.test_private_computation_id,
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        test_game_args = [
            {
                "input_base_path": private_computation_instance.compute_stage_output_base_path,
                "metrics_format_type": "lift",
                "num_shards": self.test_num_containers * NUM_NEW_SHARDS_PER_FILE,
                "output_path": private_computation_instance.shard_aggregate_stage_output_path,
                "threshold": private_computation_instance.k_anonymity_threshold,
                "run_name": "",
            }
        ]
        # check a new MPC instance handling metrics aggregation was to be created
        self.assertEqual(
            GameNames.SHARD_AGGREGATOR.value,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_name"],
        )
        self.assertEqual(
            test_game_args,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_args"],
        )
        self.private_computation_service.instance_repository.update.assert_called()
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            private_computation_instance.status,
        )

    def test_aggregate_shards_rerun(self):
        # construct a private_computation_instance
        test_private_computation_id = "test_private_computation_id"
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_private_computation_id + "_aggregate_shards",
            game_name=GameNames.SHARD_AGGREGATOR.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            instances=[mpc_instance],
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=mpc_instance
        )

        self.private_computation_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_shards
        self.private_computation_service.aggregate_shards(
            instance_id=test_private_computation_id,
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        # check that the retry counter has been incremented
        self.assertEqual(private_computation_instance.retry_counter, 1)

        # check a new MPC instance handling metrics aggregation was to be created
        self.assertEqual(2, len(private_computation_instance.instances))
        self.assertEqual(
            test_private_computation_id + "_aggregate_shards1",
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            private_computation_instance.status,
        )

    def test_aggregate_shards_dry_run(self):
        # construct a private_computation_instance
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        self.private_computation_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_shards with ad-hoc input_path and num_shards
        test_format_type = "lift"
        test_game_args = [
            {
                "input_base_path": private_computation_instance.compute_stage_output_base_path,
                "num_shards": self.test_num_containers * NUM_NEW_SHARDS_PER_FILE,
                "metrics_format_type": test_format_type,
                "output_path": private_computation_instance.shard_aggregate_stage_output_path,
                "threshold": private_computation_instance.k_anonymity_threshold,
                "run_name": "",
            }
        ]
        self.private_computation_service.aggregate_shards(
            instance_id=self.test_private_computation_id,
            server_ips=["192.0.2.0", "192.0.2.1"],
            dry_run=True,
        )

        # check a new MPC instance handling metrics aggregation was to be created
        # with the overwritten input_path and num_shards
        self.assertEqual(
            GameNames.SHARD_AGGREGATOR.value,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_name"],
        )
        self.assertEqual(
            test_game_args,
            self.private_computation_service._create_and_start_mpc_instance.call_args[
                1
            ]["game_args"],
        )
        self.private_computation_service.instance_repository.update.assert_called()
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            private_computation_instance.status,
        )

    @to_sync
    async def test_create_and_start_mpc_instance(self):
        self.private_computation_service.mpc_svc.create_instance = MagicMock()
        self.private_computation_service.mpc_svc.start_instance_async = AsyncMock()

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

        await self.private_computation_service._create_and_start_mpc_instance(
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
            self.private_computation_service.mpc_svc.create_instance.call_args,
        )

        self.assertEqual(
            call(
                instance_id=instance_id,
                server_ips=server_ips,
                timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
                version=binary_version,
            ),
            self.private_computation_service.mpc_svc.start_instance_async.call_args,
        )

    def test_map_private_computation_role_to_mpc_party(self):
        self.assertEqual(
            MPCParty.SERVER,
            self.private_computation_service._map_private_computation_role_to_mpc_party(
                PrivateComputationRole.PUBLISHER
            ),
        )
        self.assertEqual(
            MPCParty.CLIENT,
            self.private_computation_service._map_private_computation_role_to_mpc_party(
                PrivateComputationRole.PARTNER
            ),
        )

    def test_map_private_computation_role_to_pid_role(self):
        self.assertEqual(
            PIDRole.PUBLISHER,
            self.private_computation_service._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PUBLISHER
            ),
        )
        self.assertEqual(
            PIDRole.PARTNER,
            self.private_computation_service._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PARTNER
            ),
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
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            self.private_computation_service._get_status_from_stage(mpc_instance),
        )

        # Test get status from the PID stage
        pid_instance = PIDInstance(
            instance_id="test_pid_id",
            protocol=PIDProtocol.UNION_PID,
            pid_role=PIDRole.PUBLISHER,
            num_shards=4,
            input_path="input",
            output_path="output",
            stages_containers={},
            stages_status={},
            status=PIDInstanceStatus.COMPLETED,
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            self.private_computation_service._get_status_from_stage(pid_instance),
        )

    def test_prepare_data(self):
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.CREATED,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        with patch.object(
            CppLiftIdSpineCombinerService,
            "combine_on_container_async",
        ) as mock_combine, patch.object(
            CppShardingService,
            "shard_on_container_async",
        ) as mock_shard:
            # call prepare_data
            self.private_computation_service.prepare_data(
                instance_id=self.test_private_computation_id,
                dry_run=True,
            )
            binary_config = self.onedocker_binary_config_map[
                OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value
            ]
            mock_combine.assert_called_once_with(
                spine_path=private_computation_instance.pid_stage_output_spine_path,
                data_path=private_computation_instance.pid_stage_output_data_path,
                output_path=private_computation_instance.data_processing_output_path
                + "_combine",
                num_shards=self.test_num_containers,
                onedocker_svc=self.onedocker_service,
                binary_version=binary_config.binary_version,
                tmp_directory=binary_config.tmp_directory,
            )
            mock_shard.assert_called()

    def test_prepare_data_tasks_skipped(self):
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
        )
        private_computation_instance.partial_container_retry_enabled = True
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        with patch.object(
            CppLiftIdSpineCombinerService,
            "combine_on_container_async",
        ) as mock_combine, patch.object(
            CppShardingService,
            "shard_on_container_async",
        ) as mock_shard:
            # call prepare_data
            self.private_computation_service.prepare_data(
                instance_id=self.test_private_computation_id,
            )
            # expect combining and sharding skipped because this private_computation_instance has
            #   status PrivateComputationInstanceStatus.COMPUTATION_FAILED, so this run
            #   is to recover from a previous compute metrics failure, meaning data
            #   preparation should have been done
            mock_combine.assert_not_called()
            mock_shard.assert_not_called()

    def test_validate_metrics_results_doesnt_match(self):
        self.private_computation_service.mpc_svc.storage_svc.read = MagicMock()
        self.private_computation_service.mpc_svc.storage_svc.read.side_effect = [
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

        game_args = self.private_computation_service._gen_game_args_to_retry(
            private_computation_instance
        )

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
        )
