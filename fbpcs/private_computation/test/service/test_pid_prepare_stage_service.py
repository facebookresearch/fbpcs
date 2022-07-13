#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import itertools
from typing import List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig

from fbpcs.pcf.tests.async_utils import AsyncMock, to_sync
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
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
from fbpcs.private_computation.service.constants import (
    DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT,
)
from fbpcs.private_computation.service.pid_prepare_stage_service import (
    PIDPrepareStageService,
)


class TestPIDPrepareStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, mock_onedocker_svc, mock_storage_svc) -> None:
        self.mock_onedocker_svc = mock_onedocker_svc
        self.mock_storage_svc = mock_storage_svc
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp",
            binary_version="latest",
            repository_path="test_path/",
        )
        self.binary_name = "data_processing/pid_preparer"
        self.onedocker_binary_config_map = {
            self.binary_name: self.onedocker_binary_config
        }
        self.input_path = "in"
        self.output_path = "out"
        self.pc_instance_id = "test_instance_123"
        self.container_timeout = 43200

    @to_sync
    async def test_pid_prepare_stage_service(self) -> None:
        async def _run_sub_test(
            pc_role: PrivateComputationRole,
            multikey_enabled: bool,
            test_num_containers: int,
        ) -> None:
            pid_protocol = (
                PIDProtocol.UNION_PID_MULTIKEY
                if test_num_containers == 1 and multikey_enabled
                else PIDProtocol.UNION_PID
            )
            max_col_cnt_expect = (
                DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT
                if pid_protocol is PIDProtocol.UNION_PID_MULTIKEY
                else 1
            )
            pc_instance = self.create_sample_pc_instance(
                pc_role=pc_role,
                test_num_containers=test_num_containers,
                multikey_enabled=multikey_enabled,
                pid_max_column_count=max_col_cnt_expect,
            )
            stage_svc = PIDPrepareStageService(
                storage_svc=self.mock_storage_svc,
                onedocker_svc=self.mock_onedocker_svc,
                onedocker_binary_config_map=self.onedocker_binary_config_map,
            )
            containers = [
                self.create_container_instance(i) for i in range(test_num_containers)
            ]
            self.mock_onedocker_svc.start_containers = MagicMock(
                return_value=containers
            )
            self.mock_onedocker_svc.wait_for_pending_containers = AsyncMock(
                return_value=containers
            )
            updated_pc_instance = await stage_svc.run_async(pc_instance=pc_instance)
            env_vars = {
                "ONEDOCKER_REPOSITORY_PATH": self.onedocker_binary_config.repository_path
            }
            args_ls_expect = self.get_args_expected(
                pc_role, test_num_containers, max_col_cnt_expect
            )
            # test the start_containers is called with expected parameters
            self.mock_onedocker_svc.start_containers.assert_called_with(
                package_name=self.binary_name,
                version=self.onedocker_binary_config.binary_version,
                cmd_args_list=args_ls_expect,
                timeout=self.container_timeout,
                env_vars=env_vars,
            )
            # test the return value is as expected
            self.assertEqual(
                len(updated_pc_instance.infra_config.instances),
                1,
                "Failed to add the StageStateInstance into pc_instance",
            )
            stage_state_expect = StageStateInstance(
                pc_instance.infra_config.instance_id,
                pc_instance.current_stage.name,
                containers=containers,
            )
            stage_state_actual = updated_pc_instance.infra_config.instances[0]
            self.assertEqual(
                stage_state_actual,
                stage_state_expect,
                "Appended StageStateInstance is not as expected",
            )

        data_tests = itertools.product(
            [PrivateComputationRole.PUBLISHER, PrivateComputationRole.PARTNER],
            [True, False],
            [1, 2],
        )
        for pc_role, multikey_enabled, test_num_containers in data_tests:
            with self.subTest(
                pc_role=pc_role,
                multikey_enabled=multikey_enabled,
                test_num_containers=test_num_containers,
            ):
                await _run_sub_test(
                    pc_role=pc_role,
                    multikey_enabled=multikey_enabled,
                    test_num_containers=test_num_containers,
                )

    def create_sample_pc_instance(
        self,
        pc_role: PrivateComputationRole = PrivateComputationRole.PUBLISHER,
        test_num_containers: int = 1,
        pid_max_column_count: int = 1,
        multikey_enabled: bool = False,
        status: PrivateComputationInstanceStatus = PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.pc_instance_id,
            role=pc_role,
            instances=[],
            status=status,
            status_update_ts=1600000000,
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=test_num_containers,
            num_mpc_containers=test_num_containers,
            num_files_per_mpc_container=test_num_containers,
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path=self.input_path,
            output_dir=self.output_path,
            pid_use_row_numbers=True,
            pid_max_column_count=pid_max_column_count,
            multikey_enabled=multikey_enabled,
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )

    def create_container_instance(
        self,
        id: int,
        container_status: ContainerInstanceStatus = ContainerInstanceStatus.COMPLETED,
    ) -> ContainerInstance:
        return ContainerInstance(
            instance_id=f"test_container_instance_{id}",
            ip_address=f"127.0.0.{id}",
            status=container_status,
        )

    def get_args_expected(
        self,
        pc_role: PrivateComputationRole,
        test_num_containers: int,
        max_col_cnt_expected: int,
    ) -> List[str]:
        arg_ls = []
        if pc_role is PrivateComputationRole.PUBLISHER:
            arg_ls = [
                f"--input_path=out/test_instance_123_out_dir/pid_stage/out.csv_publisher_sharded_{i} --output_path=out/test_instance_123_out_dir/pid_stage/out.csv_publisher_prepared_{i} --tmp_directory=/tmp --max_column_cnt={max_col_cnt_expected}"
                for i in range(test_num_containers)
            ]
        elif pc_role is PrivateComputationRole.PARTNER:
            arg_ls = [
                f"--input_path=out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_sharded_{i} --output_path=out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_prepared_{i} --tmp_directory=/tmp --max_column_cnt={max_col_cnt_expected}"
                for i in range(test_num_containers)
            ]
        return arg_ls
