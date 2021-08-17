#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from collections import defaultdict
from unittest.mock import MagicMock, call, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.mpc import MPCInstance, MPCInstanceStatus, MPCParty, MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpmp.data_processing.lift_id_combiner.lift_id_spine_combiner_cpp import (
    CppLiftIdSpineCombinerService,
)
from fbpmp.data_processing.sharding.sharding_cpp import CppShardingService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.onedocker_binary_names import OneDockerBinaryNames
from fbpmp.onedocker_service_config import OneDockerServiceConfig
from fbpmp.pcf.tests.async_utils import to_sync
from fbpmp.pid.entity.pid_instance import (
    PIDInstance,
    PIDProtocol,
    PIDRole,
    PIDInstanceStatus,
)
from fbpmp.pid.service.pid_service.pid import PIDService
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpmp.private_lift.service.errors import PLServiceValidationError
from fbpmp.private_lift.service.privatelift import (
    PrivateLiftService,
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
)

# TODO T94666166: libfb won't work in OSS
from libfb.py.asyncio.mock import AsyncMock


class TestPrivateLiftService(unittest.TestCase):
    def setUp(self):
        container_svc_patcher = patch("fbpcp.service.container_aws.AWSContainerService")
        storage_svc_patcher = patch("fbpcp.service.storage_s3.S3StorageService")
        mpc_instance_repo_patcher = patch(
            "fbpcp.repository.mpc_instance_local.LocalMPCInstanceRepository"
        )
        pid_instance_repo_patcher = patch(
            "fbpmp.pid.repository.pid_instance_local.LocalPIDInstanceRepository"
        )
        pl_instance_repo_patcher = patch(
            "fbpmp.private_lift.repository.privatelift_instance_local.LocalPrivateLiftInstanceRepository"
        )
        mpc_game_svc_patcher = patch("fbpcp.service.mpc_game.MPCGameService")
        container_svc = container_svc_patcher.start()
        storage_svc = storage_svc_patcher.start()
        mpc_instance_repository = mpc_instance_repo_patcher.start()
        pid_instance_repository = pid_instance_repo_patcher.start()
        pl_instance_repository = pl_instance_repo_patcher.start()
        mpc_game_svc = mpc_game_svc_patcher.start()

        for patcher in (
            container_svc_patcher,
            storage_svc_patcher,
            mpc_instance_repo_patcher,
            pid_instance_repo_patcher,
            pl_instance_repo_patcher,
            mpc_game_svc_patcher,
        ):
            self.addCleanup(patcher.stop)

        self.onedocker_service_config = OneDockerServiceConfig(
            task_definition="test_task_definition",
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest"
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

        self.pl_service = PrivateLiftService(
            instance_repository=pl_instance_repository,
            mpc_svc=self.mpc_service,
            pid_svc=self.pid_service,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
        )

    def test_create_instance(self):
        test_instance_id = "test_instance_id"
        test_role = PrivateComputationRole.PUBLISHER

        self.pl_service.create_instance(instance_id=test_instance_id, role=test_role)
        # check instance_repository.create is called with the correct arguments
        self.pl_service.instance_repository.create.assert_called()
        args = self.pl_service.instance_repository.create.call_args[0][0]
        self.assertEqual(test_instance_id, args.instance_id)
        self.assertEqual(test_role, args.role)
        self.assertEqual(PrivateComputationInstanceStatus.CREATED, args.status)

    def test_update_instance(self):
        # create one MPC instance to be put into PrivateComputationInstance
        test_mpc_id = "test_mpc_id"
        mpc_instance = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name="lift",
            mpc_party=MPCParty.SERVER,
            num_workers=2,
        )
        test_pl_id = "test_pl_id"
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance],
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            status_update_ts=1600000000,
        )

        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        updated_mpc_instance = mpc_instance
        updated_mpc_instance.status = MPCInstanceStatus.COMPLETED
        self.pl_service.mpc_svc.update_instance = MagicMock(
            return_value=updated_mpc_instance
        )

        # call update on the PrivateComputationInstance
        updated_instance = self.pl_service.update_instance(instance_id=test_pl_id)

        # check update instance called on the right mpc instance
        self.pl_service.mpc_svc.update_instance.assert_called()
        self.assertEqual(
            test_mpc_id, self.pl_service.mpc_svc.update_instance.call_args[0][0]
        )

        # check update instance called on the right private lift instance
        self.pl_service.instance_repository.update.assert_called()
        self.assertEqual(
            pl_instance, self.pl_service.instance_repository.update.call_args[0][0]
        )

        # check updated_instance has new status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_COMPLETED, updated_instance.status
        )

    def test_id_match(self):
        test_pl_id = "test_pl_id"
        test_pid_id = test_pl_id + "_id_match"
        test_pid_protocol = PIDProtocol.UNION_PID
        test_num_containers = 2
        test_pl_role = PrivateComputationRole.PUBLISHER
        test_pid_role = PIDRole.PUBLISHER
        test_input_path = "pid_in"
        test_output_path = "pid_out"
        test_pid_config = {"key": "value"}
        test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="
        test_fail_fast = True

        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=test_pl_role,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=test_pid_protocol,
            pid_role=test_pid_role,
            num_shards=test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
        )
        self.pid_service.create_instance = MagicMock(return_value=pid_instance)
        self.pid_service.run_instance = AsyncMock()
        pid_instance.status = PIDInstanceStatus.STARTED
        self.pid_service.get_instance = MagicMock(return_value=pid_instance)

        # call id_match
        self.pl_service.id_match(
            instance_id=test_pl_id,
            protocol=test_pid_protocol,
            num_containers=test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
            pid_config=test_pid_config,
            fail_fast=test_fail_fast,
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
            test_num_containers,
            self.pid_service.create_instance.call_args[1]["num_shards"],
        )
        self.assertEqual(
            test_pid_role,
            self.pid_service.create_instance.call_args[1]["pid_role"],
        )
        self.assertEqual(
            test_input_path,
            self.pid_service.create_instance.call_args[1]["input_path"],
        )
        self.assertEqual(
            test_output_path,
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
        self.assertEqual(
            test_fail_fast,
            self.pid_service.run_instance.call_args[1]["fail_fast"],
        )

        self.pl_service.instance_repository.update.assert_called()

        self.assertEqual(pid_instance, pl_instance.instances[0])

    def test_id_match_rerun(self):
        # construct a pl_instance and a pid_instance
        test_pl_id = "test_pl_id"
        test_pid_id = test_pl_id + "_id_match1"
        test_pid_protocol = PIDProtocol.UNION_PID
        test_num_containers = 2
        test_input_path = "pid_in"
        test_output_path = "pid_out"
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PUBLISHER,
            instances=[],
            status=PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
            status_update_ts=1600000000,
        )
        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=test_pid_protocol,
            pid_role=PIDRole.PUBLISHER,
            num_shards=test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
            status=PIDInstanceStatus.STARTED,
        )

        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)
        self.pid_service.create_instance = MagicMock(return_value=pid_instance)
        self.pid_service.run_instance = AsyncMock()
        self.pid_service.get_instance = MagicMock(return_value=pid_instance)

        # call id_match
        self.pl_service.id_match(
            instance_id=test_pl_id,
            protocol=test_pid_protocol,
            num_containers=test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
            pid_config={"key": "value"},
            fail_fast=False,
        )

        # check that the retry counter has been incremented
        self.assertEqual(pl_instance.retry_counter, 1)

        self.assertEqual(pid_instance, pl_instance.instances[0])
        self.assertEqual(
            test_pid_id,
            self.pid_service.create_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            test_pid_id,
            self.pid_service.run_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_STARTED, pl_instance.status
        )

    def test_id_match_fail(self):
        # construct a pl_instance with the status AGGREGATION_COMPLETED
        test_pl_id = "test_pl_id"
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PUBLISHER,
            instances=[],
            status=PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        # expect an exception to be raised due to not passing status check
        with self.assertRaises(ValueError):
            self.pl_service.id_match(
                instance_id=test_pl_id,
                protocol=PIDProtocol.UNION_PID,
                num_containers=2,
                input_path="pid_in",
                output_path="pid_out",
                pid_config={"key": "value"},
                fail_fast=True,
            )

    def test_id_match_rerun_fail(self):
        # construct a pl_instance with the status ID_MATCHING_COMPLETED
        test_pl_id = "test_pl_id"
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PUBLISHER,
            instances=[],
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        # expect an exception to be raised because rerun is only allowed on ID_MATCHING_FAILED
        with self.assertRaises(ValueError):
            self.pl_service.id_match(
                instance_id=test_pl_id,
                protocol=PIDProtocol.UNION_PID,
                num_containers=2,
                input_path="pid_in",
                output_path="pid_out",
                pid_config={"key": "value"},
                fail_fast=True,
            )

    def test_compute_metrics(self):
        test_pl_id = "test_pl_id"
        test_mpc_id = test_pl_id + "_compute_metrics"
        test_game_name = "lift"
        test_num_containers = 3
        test_num_files = 5
        test_mpc_party = MPCParty.CLIENT
        test_input_base_path = "indir/infile"
        test_output_base_path = "outdir/outfile"
        test_concurrency = 2
        test_server_ips = ["192.0.2.0", "192.0.2.1", "192.0.2.2"]
        test_game_args = [
            {
                "input_base_path": test_input_base_path,
                "output_base_path": test_output_base_path,
                "file_start_index": 0,
                "num_files": 2,
                "concurrency": test_concurrency,
            },
            {
                "input_base_path": test_input_base_path,
                "output_base_path": test_output_base_path,
                "file_start_index": 2,
                "num_files": 2,
                "concurrency": test_concurrency,
            },
            {
                "input_base_path": test_input_base_path,
                "output_base_path": test_output_base_path,
                "file_start_index": 4,
                "num_files": 1,
                "concurrency": test_concurrency,
            },
        ]

        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        # construct an MPC instance as the mocked object returned by _create_and_start_mpc_instance
        mpc_instance = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=test_num_containers,
        )
        self.pl_service._create_and_start_mpc_instance = AsyncMock(
            return_value=mpc_instance
        )

        # call compute_metrics
        self.pl_service.compute_metrics(
            instance_id=test_pl_id,
            game_name=test_game_name,
            num_containers=test_num_containers,
            input_files=[f"{test_input_base_path}_{i}" for i in range(test_num_files)],
            output_files=[
                f"{test_output_base_path}_{i}" for i in range(test_num_files)
            ],
            concurrency=test_concurrency,
            server_ips=test_server_ips,
        )

        self.assertEqual(
            test_mpc_id,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            test_game_name,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_name"],
        )
        self.assertEqual(
            test_num_containers,
            self.pl_service._create_and_start_mpc_instance.call_args[1][
                "num_containers"
            ],
        )
        self.assertEqual(
            test_mpc_party,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["mpc_party"],
        )
        self.assertEqual(
            test_server_ips,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["server_ips"],
        )
        self.assertEqual(
            test_game_args,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_args"],
        )

        self.pl_service.instance_repository.update.assert_called()
        self.assertEqual(mpc_instance, pl_instance.instances[0])
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_STARTED, pl_instance.status
        )

    def test_compute_metrics_rerun(self):
        # construct a pl_instance
        test_pl_id = "test_pl_id"
        test_mpc_id = test_pl_id + "_compute_metrics"
        test_game_name = "lift"
        test_num_containers = 2
        mpc_instance = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=MPCParty.CLIENT,
            num_workers=test_num_containers,
            status=MPCInstanceStatus.FAILED,
        )
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance],
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)
        self.pl_service.mpc_svc.update_instance = MagicMock(return_value=mpc_instance)

        self.pl_service._create_and_start_mpc_instance = AsyncMock()

        # call compute_metrics
        self.pl_service.compute_metrics(
            instance_id=test_pl_id,
            game_name=test_game_name,
            num_containers=test_num_containers,
            input_files=["infile_0", "infile_1"],
            output_files=["outfile_0", "outfile_1"],
            concurrency=2,
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        # check that the retry counter has been incremented
        self.assertEqual(pl_instance.retry_counter, 1)

        # check a new MPC instance handling metrics computation was to be created
        self.assertEqual(2, len(pl_instance.instances))
        self.assertEqual(
            test_pl_id + "_compute_metrics1",
            self.pl_service._create_and_start_mpc_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_STARTED, pl_instance.status
        )

    def test_partner_missing_server_ips(self):
        test_pl_id = "test_pl_id"
        test_game_name = "lift"
        test_num_containers = 3
        test_concurrency = 2

        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        # exception because role is partner but server ips are not given
        with self.assertRaises(ValueError):
            self.pl_service.compute_metrics(
                instance_id=test_pl_id,
                game_name=test_game_name,
                num_containers=test_num_containers,
                input_files=[],
                output_files=[],
                concurrency=test_concurrency,
            )

        # exception because role is partner but server ips are not given
        with self.assertRaises(ValueError):
            self.pl_service.aggregate_metrics(
                instance_id=test_pl_id,
                output_path="output_path",
            )

    def test_aggregate_metrics(self):
        # construct a pl_instance with an mpc_instance handling metrics computation
        test_pl_id = "test_pl_id"
        test_mpc_id = test_pl_id + "_compute_metrics"
        test_output_file = "test_output_file"
        test_num_containers = 2
        test_num_shards = 80
        mpc_instance = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name="lift",
            mpc_party=MPCParty.SERVER,
            num_workers=test_num_containers,
            status=MPCInstanceStatus.COMPLETED,
        )
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance],
            status=PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            status_update_ts=1600000000,
            compute_output_path=test_output_file,
            compute_num_shards=test_num_shards,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)
        self.pl_service.mpc_svc.update_instance = MagicMock(return_value=mpc_instance)

        self.pl_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_metrics
        self.pl_service.aggregate_metrics(
            instance_id=test_pl_id,
            output_path="output_path",
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        test_game_args = [
            {
                "input_base_path": test_output_file,
                "num_shards": test_num_shards,
                "metrics_format_type": "lift",
                "output_path": "output_path",
            }
        ]
        # check a new MPC instance handling metrics aggregation was to be created
        self.assertEqual(
            "shard_aggregator",
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_name"],
        )
        self.assertEqual(
            test_game_args,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_args"],
        )
        self.pl_service.instance_repository.update.assert_called()
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED, pl_instance.status
        )

    def test_aggregate_metrics_rerun(self):
        # construct a pl_instance
        test_pl_id = "test_pl_id"
        test_compute_output_path = "test_output_file"
        mpc_instance = MPCInstance.create_instance(
            instance_id=test_pl_id + "_aggregate_metrics",
            game_name="shard_aggregator",
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
        )
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance],
            status=PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            status_update_ts=1600000000,
            compute_output_path=test_compute_output_path,
            compute_num_shards=80,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)
        self.pl_service.mpc_svc.update_instance = MagicMock(return_value=mpc_instance)

        self.pl_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_metrics
        self.pl_service.aggregate_metrics(
            instance_id=test_pl_id,
            output_path="output_path",
            server_ips=["192.0.2.0", "192.0.2.1"],
        )

        # check that the retry counter has been incremented
        self.assertEqual(pl_instance.retry_counter, 1)

        # check a new MPC instance handling metrics aggregation was to be created
        self.assertEqual(2, len(pl_instance.instances))
        self.assertEqual(
            test_pl_id + "_aggregate_metrics1",
            self.pl_service._create_and_start_mpc_instance.call_args[1]["instance_id"],
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED, pl_instance.status
        )

    def test_aggregate_metrics_dry_run(self):
        # construct a pl_instance
        test_pl_id = "test_pl_id"
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        self.pl_service._create_and_start_mpc_instance = AsyncMock()

        # call aggregate_metrics with ad-hoc input_path and num_shards
        test_output_path = "test_output_path"
        test_input_path = "input_overwrite"
        test_num_shards = 10
        test_format_type = "lift"
        test_game_args = [
            {
                "input_base_path": test_input_path,
                "metrics_format_type": test_format_type,
                "num_shards": test_num_shards,
                "output_path": test_output_path,
            }
        ]
        self.pl_service.aggregate_metrics(
            instance_id=test_pl_id,
            output_path=test_output_path,
            input_path=test_input_path,
            num_shards=test_num_shards,
            server_ips=["192.0.2.0", "192.0.2.1"],
            dry_run=True,
        )

        # check a new MPC instance handling metrics aggregation was to be created
        # with the overwritten input_path and num_shards
        self.assertEqual(
            "shard_aggregator",
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_name"],
        )
        self.assertEqual(
            test_game_args,
            self.pl_service._create_and_start_mpc_instance.call_args[1]["game_args"],
        )
        self.pl_service.instance_repository.update.assert_called()
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED, pl_instance.status
        )

    @to_sync
    async def test_create_and_start_mpc_instance(self):
        self.pl_service.mpc_svc.create_instance = MagicMock()
        self.pl_service.mpc_svc.start_instance_async = AsyncMock()

        instance_id = "test_instance_id"
        game_name = "lift"
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

        await self.pl_service._create_and_start_mpc_instance(
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
            self.pl_service.mpc_svc.create_instance.call_args,
        )

        self.assertEqual(
            call(
                instance_id=instance_id,
                server_ips=server_ips,
                timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
                version=binary_version,
            ),
            self.pl_service.mpc_svc.start_instance_async.call_args,
        )

    def test_map_pl_role_to_mpc_party(self):
        self.assertEqual(
            MPCParty.SERVER,
            self.pl_service._map_pl_role_to_mpc_party(PrivateComputationRole.PUBLISHER),
        )
        self.assertEqual(
            MPCParty.CLIENT,
            self.pl_service._map_pl_role_to_mpc_party(PrivateComputationRole.PARTNER),
        )

    def test_map_pl_role_to_pid_role(self):
        self.assertEqual(
            PIDRole.PUBLISHER,
            self.pl_service._map_pl_role_to_pid_role(PrivateComputationRole.PUBLISHER),
        )
        self.assertEqual(
            PIDRole.PARTNER,
            self.pl_service._map_pl_role_to_pid_role(PrivateComputationRole.PARTNER),
        )

    def test_get_status_from_stage(self):
        # Test get status from an MPC stage
        mpc_instance = MPCInstance.create_instance(
            instance_id="test_mpc_id",
            game_name="shard_aggregator",
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            self.pl_service._get_status_from_stage(mpc_instance),
        )

        # Test get status from the PID stage
        pid_instance = PIDInstance(
            instance_id="test_pid_id",
            protocol=PIDProtocol.UNION_PID,
            pid_role=PIDRole.PUBLISHER,
            num_shards=4,
            input_path="input",
            output_path="output",
            containers=[],
            stages_status={},
            status=PIDInstanceStatus.COMPLETED,
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            self.pl_service._get_status_from_stage(pid_instance),
        )

    def test_prepare_data(self):
        test_pl_id = "test_pl_id"
        test_num_containers = 2
        test_spine_path = "spine_path"
        test_data_path = "data_path"
        test_intermediate_output_path = "out_path_combine"
        test_output_path = "out_path"

        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            spine_path=test_spine_path,
            data_path=test_data_path,
            num_containers=test_num_containers,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        with patch.object(
            CppLiftIdSpineCombinerService,
            "combine_on_container_async",
        ) as mock_combine, patch.object(
            CppShardingService,
            "shard_on_container_async",
        ) as mock_shard:
            # call prepare_data
            self.pl_service.prepare_data(
                instance_id=test_pl_id,
                output_path=test_output_path,
                dry_run=True,
            )
            binary_config = self.onedocker_binary_config_map[
                OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value
            ]
            mock_combine.assert_called_once_with(
                spine_path=test_spine_path,
                data_path=test_data_path,
                output_path=test_intermediate_output_path,
                num_shards=test_num_containers,
                onedocker_svc=self.onedocker_service,
                binary_version=binary_config.binary_version,
                tmp_directory=binary_config.tmp_directory,
            )
            mock_shard.assert_called()

    def test_prepare_data_tasks_skipped(self):
        test_pl_id = "test_pl_id"
        test_num_containers = 2
        test_spine_path = "spine_path"
        test_data_path = "data_path"
        test_output_path = "out_path"

        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            status_update_ts=1600000000,
            partial_container_retry_enabled=True,
            spine_path=test_spine_path,
            data_path=test_data_path,
            num_containers=test_num_containers,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        with patch.object(
            CppLiftIdSpineCombinerService,
            "combine_on_container_async",
        ) as mock_combine, patch.object(
            CppShardingService,
            "shard_on_container_async",
        ) as mock_shard:
            # call prepare_data
            self.pl_service.prepare_data(
                instance_id=test_pl_id,
                output_path=test_output_path,
            )
            # expect combining and sharding skipped because this pl_instance has
            #   status PrivateComputationInstanceStatus.COMPUTATION_FAILED, so this run
            #   is to recover from a previous compute metrics failure, meaning data
            #   preparation should have been done
            mock_combine.assert_not_called()
            mock_shard.assert_not_called()

    def test_validate_metrics_results_doesnt_match(self):
        self.pl_service.mpc_svc.storage_svc.read = MagicMock()
        self.pl_service.mpc_svc.storage_svc.read.side_effect = [
            '{"subGroupMetrics":[],"metrics":{"controlClicks":1,"testSpend":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
            '{"subGroupMetrics":[],"metrics":{"testSpend":0,"controlClicks":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
        ]
        with self.assertRaises(PLServiceValidationError):
            self.pl_service.validate_metrics(
                instance_id="test_id",
                aggregated_result_path="aggregated_result_path",
                expected_result_path="expected_result_path",
            )

    def test_cancel_current_stage(self):
        test_pl_id = "test_pl_id"
        test_mpc_id = test_pl_id + "_compute_metrics"
        test_game_name = "lift"
        test_num_containers = 3
        test_mpc_party = MPCParty.CLIENT

        # prepare the pl instance that will be read in to memory from the repository
        # at the beginning of the cancel_current_stage function
        mpc_instance_started = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=test_num_containers,
            status=MPCInstanceStatus.STARTED,
        )
        pl_instance = PrivateComputationInstance(
            instance_id=test_pl_id,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance_started],
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            status_update_ts=1600000000,
        )
        self.pl_service.instance_repository.read = MagicMock(return_value=pl_instance)

        # prepare the mpc instance that's returned from mpc_service.stop_instance()
        mpc_instance_canceled = MPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=test_num_containers,
            status=MPCInstanceStatus.CANCELED,
        )
        self.pl_service.mpc_svc.stop_instance = MagicMock(
            return_value=mpc_instance_canceled
        )
        self.pl_service.mpc_svc.instance_repository.read = MagicMock(
            return_value=mpc_instance_canceled
        )

        # call cancel, expect no exception
        pl_instance = self.pl_service.cancel_current_stage(
            instance_id=test_pl_id,
        )

        # assert the pl instance returned has the correct status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_FAILED, pl_instance.status
        )

    def test_calculate_file_start_index_and_num_shards(self):
        self.assertEqual(
            list(self.pl_service.calculate_file_start_index_and_num_shards([0] * 4, 4)),
            [(0, 1), (1, 1), (2, 1), (3, 1)],
        )
        self.assertEqual(
            list(self.pl_service.calculate_file_start_index_and_num_shards([0] * 5, 4)),
            [(0, 2), (2, 1), (3, 1), (4, 1)],
        )
        self.assertEqual(
            list(self.pl_service.calculate_file_start_index_and_num_shards([0] * 6, 4)),
            [(0, 2), (2, 2), (4, 1), (5, 1)],
        )
        self.assertEqual(
            list(self.pl_service.calculate_file_start_index_and_num_shards([0] * 7, 4)),
            [(0, 2), (2, 2), (4, 2), (6, 1)],
        )
        self.assertEqual(
            list(self.pl_service.calculate_file_start_index_and_num_shards([0] * 8, 4)),
            [(0, 2), (2, 2), (4, 2), (6, 2)],
        )

    def test_gen_game_args_to_retry(self):
        test_input = "test_input_retry"
        mpc_instance = MPCInstance.create_instance(
            instance_id="mpc_instance",
            game_name="lift",
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
        pl_instance = PrivateComputationInstance(
            instance_id="instance_id",
            role=PrivateComputationRole.PUBLISHER,
            instances=[mpc_instance],
            is_validating=False,
            status=PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            status_update_ts=1600000000,
        )

        game_args = self.pl_service._gen_game_args_to_retry(pl_instance)

        self.assertEqual(1, len(game_args))  # only 1 failed container
        self.assertEqual(test_input, game_args[0]["input_filenames"])
