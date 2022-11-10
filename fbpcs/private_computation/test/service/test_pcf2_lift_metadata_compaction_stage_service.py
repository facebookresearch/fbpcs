#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from fbpcp.entity.mpc_instance import MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider

from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE
from fbpcs.private_computation.service.mpc.mpc import MPCService

from fbpcs.private_computation.service.pcf2_lift_metadata_compaction_stage_service import (
    PCF2LiftMetadataCompactionStageService,
)


class TestPCF2LiftMetadataCompactionStageService(IsolatedAsyncioTestCase):
    @patch("fbpcs.private_computation.service.mpc.mpc.MPCService")
    def setUp(self, mock_mpc_svc: MPCService) -> None:
        self.mock_mpc_svc = mock_mpc_svc
        self.mock_mpc_svc.get_instance = MagicMock(side_effect=Exception())
        self.mock_mpc_svc.create_instance = MagicMock()

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = PCF2LiftMetadataCompactionStageService(
            onedocker_binary_config_map,
            self.mock_mpc_svc,
        )

    async def test_run_async_with_udp(self) -> None:
        private_computation_instance = self._create_pc_instance()
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=private_computation_instance.infra_config.instance_id
            + "_pcf2_lift_metadata_compaction",
            game_name=GameNames.PCF2_LIFT_METADATA_COMPACTION.value,
            mpc_party=MPCParty.CLIENT,
            num_workers=private_computation_instance.infra_config.num_mpc_containers,
        )

        self.mock_mpc_svc.start_instance_async = AsyncMock(return_value=mpc_instance)

        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        await self.stage_svc.run_async(
            private_computation_instance,
            NullCertificateProvider(),
            NullCertificateProvider(),
            "",
            "",
            test_server_ips,
        )

        self.assertEqual(
            mpc_instance, private_computation_instance.infra_config.instances[0]
        )

    def test_get_game_args_with_udp(self) -> None:
        private_computation_instance = self._create_pc_instance()
        base_run_name = (
            private_computation_instance.infra_config.instance_id
            + "_"
            + GameNames.PCF2_LIFT_METADATA_COMPACTION.value
        )

        test_game_args = [
            {
                "input_base_path": private_computation_instance.secure_random_sharder_output_base_path,
                "output_global_params_base_path": f"{private_computation_instance.pcf2_lift_metadata_compaction_output_base_path}_global_params",
                "output_secret_shares_base_path": f"{private_computation_instance.pcf2_lift_metadata_compaction_output_base_path}_secret_shares",
                "file_start_index": i
                * private_computation_instance.infra_config.num_files_per_mpc_container,
                "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
                "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
                "num_conversions_per_user": private_computation_instance.product_config.common.padding_size,
                "run_name": f"{base_run_name}_{i}",
                "log_cost": True,
                "use_tls": False,
                "ca_cert_path": "",
                "server_cert_path": "",
                "private_key_path": "",
                "pc_feature_flags": "private_lift_unified_data_process",
                "log_cost_s3_bucket": private_computation_instance.infra_config.log_cost_bucket,
            }
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        self.assertEqual(
            test_game_args,
            self.stage_svc._get_lift_metadata_compaction_game_args(
                private_computation_instance, "", ""
            ),
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:

        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=4,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            pcs_features={PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS},
            log_cost_bucket="test_log_cost_bucket",
        )

        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
