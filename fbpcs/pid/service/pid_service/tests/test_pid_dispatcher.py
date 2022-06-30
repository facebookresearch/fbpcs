#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

from fbpcp.service.container_aws import AWSContainerService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pcf.tests.async_utils import AsyncMock, to_sync
from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDProtocol,
    PIDRole,
    PIDStageStatus,
)
from fbpcs.pid.entity.pid_stages import (
    PIDFlowUnsupportedError,
    PIDStageFailureError,
    UnionPIDStage,
)
from fbpcs.pid.service.coordination.file_coordination import FileCoordinationService
from fbpcs.pid.service.pid_service.pid_dispatcher import PIDDispatcher
from fbpcs.pid.service.pid_service.pid_execution_map import PIDFlow
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


class TestPIDDispatcher(unittest.TestCase):
    def setUp(self) -> None:
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp/",
            binary_version="latest",
            repository_path="test_path/",
        )

    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    def test_pid_flow_unsupported_protocol(
        self,
        mock_instance_repo,
    ) -> None:
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        with self.assertRaises(PIDFlowUnsupportedError):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                protocol=PIDProtocol.PS3I_M_TO_M,
                role=PIDRole.PUBLISHER,
                # pyre-fixme[6]: For 6th param expected `StorageService` but got `str`.
                storage_svc="STORAGE",
                # pyre-fixme[6]: For 7th param expected `OneDockerService` but got
                #  `str`.
                onedocker_svc="ONEDOCKER",
                # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
                #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
            )

    @patch(
        "fbpcs.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_publisher_nodes(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ) -> None:
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PUBLISHER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )
        constructed_map = {}
        for stage in dispatcher.dag.nodes:
            constructed_map[stage.stage_type] = [
                next_stage.stage_type for next_stage in dispatcher.dag.successors(stage)
            ]

        self.assertEqual(len(dispatcher.dag.nodes), 3)
        self.assertEqual(len(constructed_map), 3)
        self.assertDictEqual(
            constructed_map,
            {
                UnionPIDStage.PUBLISHER_SHARD: [
                    UnionPIDStage.PUBLISHER_PREPARE,
                ],
                UnionPIDStage.PUBLISHER_PREPARE: [UnionPIDStage.PUBLISHER_RUN_PID],
                UnionPIDStage.PUBLISHER_RUN_PID: [],
            },
        )

    @patch(
        "fbpcs.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_publisher_nodes_with_data_path_spine_path(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ) -> None:
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        # provide optional parameters (data_path and spine_path)
        # expect no impact overall
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PUBLISHER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
            data_path="data.txt",
            spine_path="spine.txt",
        )
        constructed_map = {}
        for stage in dispatcher.dag.nodes:
            constructed_map[stage.stage_type] = [
                next_stage.stage_type for next_stage in dispatcher.dag.successors(stage)
            ]

        self.assertEqual(len(dispatcher.dag.nodes), 3)
        self.assertEqual(len(constructed_map), 3)
        self.assertDictEqual(
            constructed_map,
            {
                UnionPIDStage.PUBLISHER_SHARD: [
                    UnionPIDStage.PUBLISHER_PREPARE,
                ],
                UnionPIDStage.PUBLISHER_PREPARE: [UnionPIDStage.PUBLISHER_RUN_PID],
                UnionPIDStage.PUBLISHER_RUN_PID: [],
            },
        )

    @patch(
        "fbpcs.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_partner_nodes(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ) -> None:
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PARTNER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )
        constructed_map = {}
        for stage in dispatcher.dag.nodes:
            constructed_map[stage.stage_type] = [
                next_stage.stage_type for next_stage in dispatcher.dag.successors(stage)
            ]

        self.assertEqual(len(dispatcher.dag.nodes), 3)
        self.assertEqual(len(constructed_map), 3)
        self.assertDictEqual(
            constructed_map,
            {
                UnionPIDStage.ADV_SHARD: [
                    UnionPIDStage.ADV_PREPARE,
                ],
                UnionPIDStage.ADV_PREPARE: [UnionPIDStage.ADV_RUN_PID],
                UnionPIDStage.ADV_RUN_PID: [],
            },
        )

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_union_pid_run_all_order(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_shard_stage,
        mock_pid_prepare_stage,
        mock_pid_run_protocol_stage,
    ) -> None:
        complete_mock = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_shard_stage().run = complete_mock
        mock_pid_prepare_stage().run = complete_mock
        mock_pid_run_protocol_stage().run = complete_mock

        instance_id = "456"
        protocol = PIDProtocol.UNION_PID
        pid_role = PIDRole.PARTNER
        num_shards = 50
        input_path = "abc.text"
        output_path = "def.txt"

        dispatcher = PIDDispatcher(
            instance_id=instance_id, instance_repository=mock_instance_repo
        )

        sample_pid_instance = self._get_sample_pid_instance(
            instance_id=instance_id,
            protocol=protocol,
            pid_role=pid_role,
            num_shards=num_shards,
            input_path=input_path,
            output_path=output_path,
        )
        dispatcher.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )

        dispatcher.build_stages(
            input_path=input_path,
            output_path=output_path,
            num_shards=num_shards,
            protocol=protocol,
            role=pid_role,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        # pre-run DAG should have 3 nodes
        self.assertEqual(len(dispatcher.dag.nodes), 3)
        await dispatcher.run_all()
        # post run DAG should be empty
        self.assertEqual(len(dispatcher.dag.nodes), 0)
        # Expect each (mocked) node to have called run() once
        self.assertEqual(complete_mock.mock.call_count, 3)

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_pid_run_stage_with_exception(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_shard_stage,
        mock_pid_prepare_stage,
    ) -> None:
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)

        instance_id = "344"
        protocol = PIDProtocol.UNION_PID
        pid_role = PIDRole.PARTNER
        num_shards = 50
        input_path = "abc.text"
        output_path = "def.txt"

        dispatcher = PIDDispatcher(
            instance_id=instance_id, instance_repository=mock_instance_repo
        )

        dispatcher.build_stages(
            input_path=input_path,
            output_path=output_path,
            num_shards=num_shards,
            protocol=protocol,
            role=pid_role,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        # run pid shard stage
        await dispatcher.run_stage(mock_pid_shard_stage())
        self.assertEqual(len(dispatcher.dag.nodes), 2)

        # attempt to fail the prepare stage
        mock_pid_prepare_stage().run = Exception()
        with self.assertRaises(PIDStageFailureError):
            await dispatcher.run_stage(mock_pid_prepare_stage())
        self.assertEqual(len(dispatcher.dag.nodes), 2)

        # rerun the failed stage once again
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        await dispatcher.run_stage(mock_pid_prepare_stage())
        # verify the stage is run successfully
        self.assertEqual(len(dispatcher.dag.nodes), 1)

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_union_pid_run_stages_one_by_one(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_shard_stage,
        mock_pid_prepare_stage,
        mock_pid_run_protocol_stage,
    ) -> None:
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_run_protocol_stage().run = AsyncMock(
            return_value=PIDStageStatus.COMPLETED
        )

        instance_id = "456"
        protocol = PIDProtocol.UNION_PID
        pid_role = PIDRole.PARTNER
        num_shards = 50
        input_path = "abc.text"
        output_path = "def.txt"

        dispatcher = PIDDispatcher(
            instance_id=instance_id, instance_repository=mock_instance_repo
        )

        dispatcher.build_stages(
            input_path=input_path,
            output_path=output_path,
            num_shards=num_shards,
            protocol=protocol,
            role=pid_role,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        # pre-run DAG should have 3 nodes
        self.assertEqual(len(dispatcher.dag.nodes), 3)
        await dispatcher.run_stage(mock_pid_shard_stage())
        self.assertEqual(len(dispatcher.dag.nodes), 2)

        # attempt to run out of order
        with self.assertRaises(PIDStageFailureError):
            await dispatcher.run_stage(mock_pid_run_protocol_stage())
        # dag should not have been affected
        self.assertEqual(len(dispatcher.dag.nodes), 2)

        # continue running in correct order
        await dispatcher.run_stage(mock_pid_prepare_stage())
        self.assertEqual(len(dispatcher.dag.nodes), 1)

        await dispatcher.run_stage(mock_pid_run_protocol_stage())
        self.assertEqual(len(dispatcher.dag.nodes), 0)

        # attempt to rerun an already completed stage
        with self.assertRaises(PIDStageFailureError):
            await dispatcher.run_stage(mock_pid_run_protocol_stage())

        # each stage should only have been called once
        mock_pid_shard_stage().run.mock.assert_called_once()
        mock_pid_prepare_stage().run.mock.assert_called_once()
        mock_pid_run_protocol_stage().run.mock.assert_called_once()

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_union_pid_run_only_unfinished_stages(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_shard_stage,
        mock_pid_prepare_stage,
        mock_pid_run_protocol_stage,
    ) -> None:
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_run_protocol_stage().run = AsyncMock(
            return_value=PIDStageStatus.COMPLETED
        )

        instance_id = "456"
        protocol = PIDProtocol.UNION_PID
        pid_role = PIDRole.PARTNER
        num_shards = 50
        input_path = "abc.text"
        output_path = "def.txt"

        dispatcher = PIDDispatcher(
            instance_id=instance_id, instance_repository=mock_instance_repo
        )

        sample_pid_instance = self._get_sample_pid_instance(
            instance_id=instance_id,
            protocol=protocol,
            pid_role=pid_role,
            num_shards=num_shards,
            input_path=input_path,
            output_path=output_path,
        )
        # make the instance think it has completed the shard stage previously
        sample_pid_instance.stages_status[
            mock_pid_shard_stage().stage_type
        ] = PIDStageStatus.COMPLETED
        # make the instance think it has attempted and failed the prepare stage previously
        sample_pid_instance.stages_status[
            mock_pid_prepare_stage().stage_type
        ] = PIDStageStatus.FAILED
        dispatcher.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )

        dispatcher.build_stages(
            input_path=input_path,
            output_path=output_path,
            num_shards=num_shards,
            protocol=protocol,
            role=pid_role,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        # pre-run DAG should have 2 nodes, since PID Shard is already finished
        self.assertEqual(len(dispatcher.dag.nodes), 2)
        await dispatcher.run_all()
        # post run DAG should be empty
        self.assertEqual(len(dispatcher.dag.nodes), 0)
        # pid shard stage was already finished, so it should not be called again
        mock_pid_shard_stage().run.mock.assert_not_called()
        # prepare failed, so it should run again
        mock_pid_prepare_stage().run.mock.assert_called_once()
        # pid run was never attempted, so it should run
        mock_pid_run_protocol_stage().run.mock.assert_called_once()

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_union_pid_flow_valid_partner(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_run_protocol_stage,
        mock_pid_prepare_stage,
        mock_pid_shard_stage,
    ) -> None:
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_shard_stage().stage_type = UnionPIDStage.PUBLISHER_SHARD
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_run_protocol_stage().type = UnionPIDStage.PUBLISHER_RUN_PID
        mock_pid_run_protocol_stage().run = AsyncMock(
            return_value=PIDStageStatus.COMPLETED
        )

        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PARTNER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        self.assertEqual(len(dispatcher.dag.nodes), 3)

        await dispatcher.run_all()
        # Make sure each stage is called exactly once
        mock_pid_shard_stage().run.mock.assert_called_once()
        mock_pid_prepare_stage().run.mock.assert_called_once()
        mock_pid_run_protocol_stage().run.mock.assert_called_once()

        self.assertEqual(
            mock_pid_shard_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["abc.text"],
                output_paths=["def.txt_advertiser_sharded"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(
            mock_pid_prepare_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["def.txt_advertiser_sharded"],
                output_paths=["def.txt_advertiser_prepared"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(
            mock_pid_run_protocol_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["def.txt_advertiser_prepared"],
                output_paths=["def.txt_advertiser_pid_matched"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(len(dispatcher.dag.nodes), 0)  # all done

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_union_pid_flow_valid_partner_with_data_path_spine_path(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_pid_run_protocol_stage,
        mock_pid_prepare_stage,
        mock_pid_shard_stage,
    ) -> None:
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_shard_stage().stage_type = UnionPIDStage.PUBLISHER_SHARD
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_run_protocol_stage().stage_type = UnionPIDStage.PUBLISHER_RUN_PID
        mock_pid_run_protocol_stage().run = AsyncMock(
            return_value=PIDStageStatus.COMPLETED
        )

        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        # The stage contains two additional parameters: data_path and spine_path
        # data_path is the output of the shard stage
        # spine_path is the output of the protocol run stage
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PARTNER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
            data_path="data.txt",
            spine_path="spine.txt",
        )

        self.assertEqual(len(dispatcher.dag.nodes), 3)

        await dispatcher.run_all()
        # Make sure each stage is called exactly once
        mock_pid_shard_stage().run.mock.assert_called_once()
        mock_pid_prepare_stage().run.mock.assert_called_once()
        mock_pid_run_protocol_stage().run.mock.assert_called_once()

        # expect output_paths as specified in data_path
        self.assertEqual(
            mock_pid_shard_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["abc.text"],
                output_paths=["data.txt_advertiser_sharded"],
                num_shards=50,
                instance_id="456",
            ),
        )
        # expect input_paths as specified in data_path for shard stage
        self.assertEqual(
            mock_pid_prepare_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["data.txt_advertiser_sharded"],
                output_paths=["def.txt_advertiser_prepared"],
                num_shards=50,
                instance_id="456",
            ),
        )
        # expect output_paths as specified in spine_path for protocol run stage
        self.assertEqual(
            mock_pid_run_protocol_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["def.txt_advertiser_prepared"],
                output_paths=["spine.txt_advertiser_pid_matched"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(len(dispatcher.dag.nodes), 0)  # all done

    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpcs.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcs.pid.service.pid_service.pid_execution_map.get_execution_flow")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_valid_custom_flow(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_get_execution_flow,
        mock_pid_run_protocol_stage,
        mock_pid_prepare_stage,
        mock_pid_shard_stage,
    ) -> None:
        # custom flow with non-linear dependency
        mock_get_execution_flow.return_value = PIDFlow(
            name="union_pid_advertiser",
            base_flow="union_pid",
            # pyre-fixme[6]: For 3rd param expected `Dict[UnionPIDStage, List[str]]`
            #  but got `List[Variable[_T]]`.
            extra_args=[],
            flow={
                UnionPIDStage.ADV_SHARD: [
                    UnionPIDStage.ADV_PREPARE,
                    UnionPIDStage.ADV_RUN_PID,
                ],
                UnionPIDStage.ADV_PREPARE: [],
                UnionPIDStage.ADV_RUN_PID: [],
            },
        )
        mock_pid_prepare_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_shard_stage().stage_type = UnionPIDStage.PUBLISHER_SHARD
        mock_pid_shard_stage().run = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_run_protocol_stage().type = UnionPIDStage.PUBLISHER_RUN_PID
        mock_pid_run_protocol_stage().run = AsyncMock(
            return_value=PIDStageStatus.COMPLETED
        )

        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        dispatcher.build_stages(
            input_path="abc.text",
            output_path="def.txt",
            num_shards=50,
            protocol=PIDProtocol.UNION_PID,
            role=PIDRole.PARTNER,
            storage_svc=mock_s3_storage_service,
            onedocker_svc=mock_onedocker_service,
            # pyre-fixme[6]: For 8th param expected `DefaultDict[str,
            #  OneDockerBinaryConfig]` but got `DefaultDict[Variable[_KT], str]`.
            onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
        )

        self.assertEqual(len(dispatcher.dag.nodes), 3)

        await dispatcher.run_all()
        # Make sure each stage is called exactly once
        mock_pid_shard_stage().run.mock.assert_called_once()
        mock_pid_prepare_stage().run.mock.assert_called_once()
        mock_pid_run_protocol_stage().run.mock.assert_called_once()

        self.assertEqual(
            mock_pid_shard_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["abc.text"],
                output_paths=["def.txt_advertiser_sharded"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(
            mock_pid_prepare_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["def.txt_advertiser_sharded"],
                output_paths=["def.txt_advertiser_prepared"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(
            mock_pid_run_protocol_stage().run.mock.call_args[0][0],
            PIDStageInput(
                input_paths=["def.txt_advertiser_sharded"],
                output_paths=["def.txt_advertiser_pid_matched"],
                num_shards=50,
                instance_id="456",
            ),
        )
        self.assertEqual(len(dispatcher.dag.nodes), 0)  # all done

    def _get_sample_pid_instance(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_role: PIDRole,
        num_shards: int,
        input_path: str,
        output_path: str,
        data_path: str = "",
        spine_path: str = "",
    ) -> PIDInstance:
        return PIDInstance(
            instance_id=instance_id,
            protocol=protocol,
            pid_role=pid_role,
            num_shards=num_shards,
            input_path=input_path,
            output_path=output_path,
            data_path=data_path,
            spine_path=spine_path,
        )
