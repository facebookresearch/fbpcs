#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from collections import defaultdict
from unittest.mock import mock_open, patch, MagicMock

from fbpcp.service.container_aws import AWSContainerService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage_s3 import S3StorageService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pcf.tests.async_utils import AsyncMock
from fbpmp.pcf.tests.async_utils import to_sync
from fbpmp.pid.entity.pid_instance import PIDInstance
from fbpmp.pid.entity.pid_instance import PIDProtocol, PIDRole, PIDStageStatus
from fbpmp.pid.entity.pid_stages import PIDFlowUnsupportedError, UnionPIDStage
from fbpmp.pid.service.coordination.file_coordination import FileCoordinationService
from fbpmp.pid.service.pid_service.pid_dispatcher import PIDDispatcher
from fbpmp.pid.service.pid_service.pid_execution_map import PIDFlow
from fbpmp.pid.service.pid_service.pid_stage_input import PIDStageInput


CONFIG_TEXT = """
dependency:
    CoordinationService:
        class: fbpmp.pid.service.coordination.file_coordination.FileCoordinationService
        constructor:
            coordination_objects:
                pid_ip_addrs:
                    value: ip_config.txt
CloudCredentialService:
    class: fbpmp.pid.service.credential_service.simple_cloud_credential_service.SimpleCloudCredentialService
    constructor:
        access_key_id: key_id
        access_key_data: key_data
"""


class TestPIDDispatcher(unittest.TestCase):
    def setUp(self):
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp/",
            binary_version="latest",
        )

    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    def test_pid_flow_unsupported_protocol(
        self,
        mock_instance_repo,
    ):
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        with self.assertRaises(PIDFlowUnsupportedError):
            with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
                dispatcher.build_stages(
                    input_path="abc.text",
                    output_path="def.txt",
                    num_shards=50,
                    pid_config="config.yml",
                    protocol=PIDProtocol.PS3I_M_TO_M,
                    role=PIDRole.PUBLISHER,
                    storage_svc="STORAGE",
                    onedocker_svc="ONEDOCKER",
                    onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                    fail_fast=True,
                )

    @patch(
        "fbpmp.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_publisher_nodes(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ):
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PUBLISHER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                fail_fast=True,
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
        "fbpmp.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_publisher_nodes_with_data_path_spine_path(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ):
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        # provide optional parameters (data_path and spine_path)
        # expect no impact overall
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PUBLISHER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                data_path="data.txt",
                spine_path="spine.txt",
                fail_fast=True,
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
        "fbpmp.pid.service.coordination.file_coordination.FileCoordinationService",
        spec=FileCoordinationService,
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    def test_union_pid_flow_valid_partner_nodes(
        self,
        mock_instance_repo,
        mock_aws_container_service,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_file_coordination_service,
    ):
        dispatcher = PIDDispatcher(
            instance_id="456", instance_repository=mock_instance_repo
        )
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PARTNER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                fail_fast=True,
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

    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
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
    ):
        complete_mock = AsyncMock(return_value=PIDStageStatus.COMPLETED)
        mock_pid_shard_stage().run = complete_mock
        mock_pid_prepare_stage().run = complete_mock
        mock_pid_run_protocol_stage().run = complete_mock

        instance_id = "456"
        protocol = PIDProtocol.UNION_PID
        pid_role = PIDRole.PARTNER
        num_shards = 50
        is_validating = False
        input_path = "abc.text"
        output_path = "def.txt"
        pid_config = "config.yml"

        dispatcher = PIDDispatcher(
            instance_id=instance_id, instance_repository=mock_instance_repo
        )

        sample_pid_instance = self._get_sample_pid_instance(
            instance_id=instance_id,
            protocol=protocol,
            pid_role=pid_role,
            num_shards=num_shards,
            is_validating=is_validating,
            input_path=input_path,
            output_path=output_path,
        )
        dispatcher.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )

        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path=input_path,
                output_path=output_path,
                num_shards=num_shards,
                pid_config=pid_config,
                protocol=protocol,
                role=pid_role,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                fail_fast=True,
            )

        # pre-run DAG should have 3 nodes
        self.assertEqual(len(dispatcher.dag.nodes), 3)
        await dispatcher.run_all()
        # post run DAG should be empty
        self.assertEqual(len(dispatcher.dag.nodes), 0)
        # Expect each (mocked) node to have called run() once
        self.assertEqual(complete_mock.mock.call_count, 3)

    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
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
    ):
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
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PARTNER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                fail_fast=False,
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

    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
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
    ):
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
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PARTNER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                data_path="data.txt",
                spine_path="spine.txt",
                fail_fast=False,
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

    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDShardStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDPrepareStage")
    @patch("fbpmp.pid.service.pid_service.pid_stage_mapper.PIDProtocolRunStage")
    @patch("fbpmp.pid.service.pid_service.pid_execution_map.get_execution_flow")
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcp.service.container_aws.AWSContainerService", spec=AWSContainerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
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
    ):
        # custom flow with non-linear dependency
        mock_get_execution_flow.return_value = PIDFlow(
            name="union_pid_advertiser",
            base_flow="union_pid",
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
        with patch("builtins.open", mock_open(read_data=CONFIG_TEXT)):
            dispatcher.build_stages(
                input_path="abc.text",
                output_path="def.txt",
                num_shards=50,
                pid_config="config.yml",
                protocol=PIDProtocol.UNION_PID,
                role=PIDRole.PARTNER,
                storage_svc=mock_s3_storage_service,
                onedocker_svc=mock_onedocker_service,
                onedocker_binary_config_map=defaultdict(lambda: "OD_CONFIG"),
                fail_fast=False,
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
        is_validating: bool,
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
            is_validating=is_validating,
            input_path=input_path,
            output_path=output_path,
            data_path=data_path,
            spine_path=spine_path,
        )
