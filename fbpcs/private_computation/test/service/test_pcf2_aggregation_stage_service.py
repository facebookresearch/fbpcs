#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from collections import defaultdict
from typing import Set
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.entity.container_permission import ContainerPermissionConfig
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE
from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.pcf2_aggregation_stage_service import (
    PCF2AggregationStageService,
)


class TestPCF2AggregationStageService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_mpc_svc = MagicMock(spec=MPCService)
        self.mock_mpc_svc.onedocker_svc = MagicMock()
        self.run_id = "681ba82c-16d9-11ed-861d-0242ac120002"

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = PCF2AggregationStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )
        self.container_permission_id = "test-container-permission"

    async def test_aggregation_stage(self) -> None:
        containers = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        self.mock_mpc_svc.start_containers.return_value = containers
        private_computation_instance = self._create_pc_instance(pcs_features=set())
        binary_name = "private_attribution/pcf2_aggregation"
        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        self.mock_mpc_svc.convert_cmd_args_list.return_value = (
            binary_name,
            ["cmd_1", "cmd_2"],
        )

        # act
        await self.stage_svc.run_async(
            private_computation_instance,
            NullCertificateProvider(),
            NullCertificateProvider(),
            "",
            "",
            test_server_ips,
        )

        # asserts
        self.mock_mpc_svc.start_containers.assert_called_once_with(
            cmd_args_list=["cmd_1", "cmd_2"],
            onedocker_svc=self.mock_mpc_svc.onedocker_svc,
            binary_version="latest",
            binary_name=binary_name,
            timeout=None,
            env_vars={"ONEDOCKER_REPOSITORY_PATH": "test_path/"},
            wait_for_containers_to_start_up=True,
            existing_containers=None,
            env_vars_list=None,
            opa_workflow_path=None,
            permission=ContainerPermissionConfig(self.container_permission_id),
        )
        self.assertEqual(
            containers,
            # pyre-ignore
            private_computation_instance.infra_config.instances[-1].containers,
        )
        self.assertEqual(
            "PCF2_AGGREGATION",
            # pyre-ignore
            private_computation_instance.infra_config.instances[-1].stage_name,
        )

    def test_get_game_args(self) -> None:
        private_computation_instance = self._create_pc_instance(pcs_features=set())

        common_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "input_base_path_secret_share": private_computation_instance.pcf2_attribution_stage_output_base_path,
            "output_base_path": private_computation_instance.pcf2_aggregation_stage_output_base_path,
            "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
            "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
            "max_num_touchpoints": private_computation_instance.product_config.common.padding_size,
            "max_num_conversions": private_computation_instance.product_config.common.padding_size,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "attribution_rules": private_computation_instance.product_config.attribution_rule.value,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "aggregators": private_computation_instance.product_config.aggregation_type.value,
            "use_xor_encryption": True,
            "use_postfix": True,
            "log_cost": True,
            "run_id": self.run_id,
            "use_tls": False,
            "ca_cert_path": "",
            "server_cert_path": "",
            "private_key_path": "",
            "log_cost_s3_bucket": private_computation_instance.infra_config.log_cost_bucket,
            "use_new_output_format": False,
        }
        test_game_args = [
            {
                **common_game_args,
                "run_name": (
                    f"{private_computation_instance.infra_config.instance_id}_{GameNames.PCF2_AGGREGATION.value}_0"
                    if self.stage_svc._log_cost_to_s3
                    else ""
                ),
                "file_start_index": 0,
            },
            {
                **common_game_args,
                "run_name": (
                    f"{private_computation_instance.infra_config.instance_id}_{GameNames.PCF2_AGGREGATION.value}_1"
                    if self.stage_svc._log_cost_to_s3
                    else ""
                ),
                "file_start_index": private_computation_instance.infra_config.num_files_per_mpc_container,
            },
        ]
        actual_value = self.stage_svc.get_game_args(
            private_computation_instance, "", ""
        )
        self.assertEqual(
            test_game_args,
            actual_value,
        )

    def test_get_game_args_with_feature_flags(self) -> None:
        private_computation_instance = self._create_pc_instance(
            {PCSFeature.PRIVATE_ATTRIBUTION_REFORMATTED_OUTPUT}
        )

        common_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "input_base_path_secret_share": private_computation_instance.pcf2_attribution_stage_output_base_path,
            "output_base_path": private_computation_instance.pcf2_aggregation_stage_output_base_path,
            "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
            "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
            "max_num_touchpoints": private_computation_instance.product_config.common.padding_size,
            "max_num_conversions": private_computation_instance.product_config.common.padding_size,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "attribution_rules": private_computation_instance.product_config.attribution_rule.value,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "aggregators": private_computation_instance.product_config.aggregation_type.value,
            "use_xor_encryption": True,
            "use_postfix": True,
            "log_cost": True,
            "run_id": self.run_id,
            "use_tls": False,
            "ca_cert_path": "",
            "server_cert_path": "",
            "private_key_path": "",
            "log_cost_s3_bucket": private_computation_instance.infra_config.log_cost_bucket,
            "use_new_output_format": True,
            "pc_feature_flags": "private_attribution_reformatted_output",
        }
        test_game_args = [
            {
                **common_game_args,
                "run_name": (
                    f"{private_computation_instance.infra_config.instance_id}_{GameNames.PCF2_AGGREGATION.value}_0"
                    if self.stage_svc._log_cost_to_s3
                    else ""
                ),
                "file_start_index": 0,
            },
            {
                **common_game_args,
                "run_name": (
                    f"{private_computation_instance.infra_config.instance_id}_{GameNames.PCF2_AGGREGATION.value}_1"
                    if self.stage_svc._log_cost_to_s3
                    else ""
                ),
                "file_start_index": private_computation_instance.infra_config.num_files_per_mpc_container,
            },
        ]
        actual_value = self.stage_svc.get_game_args(
            private_computation_instance, "", ""
        )
        self.assertEqual(
            test_game_args,
            actual_value,
        )

    def _create_pc_instance(
        self, pcs_features: Set[PCSFeature]
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            _stage_flow_cls_name="PrivateComputationPCF2StageFlow",
            status=PrivateComputationInstanceStatus.PCF2_AGGREGATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.ATTRIBUTION,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            run_id=self.run_id,
            log_cost_bucket="test_log_cost_bucket",
            pcs_features=pcs_features,
            container_permission_id=self.container_permission_id,
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
            padding_size=4,
        )
        product_config: ProductConfig = AttributionConfig(
            common=common,
            attribution_rule=AttributionRule.LAST_CLICK_1D,
            aggregation_type=AggregationType.MEASUREMENT,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
