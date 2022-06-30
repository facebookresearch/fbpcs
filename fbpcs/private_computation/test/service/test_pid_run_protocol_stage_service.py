#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import copy
import itertools
from collections import defaultdict
from typing import List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.data_processing.service.pid_run_protocol_binary_service import (
    PIDRunProtocolBinaryService,
)
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.pcf.tests.async_utils import AsyncMock, to_sync
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.constants import DEFAULT_CONTAINER_TIMEOUT_IN_SEC
from fbpcs.private_computation.service.pid_run_protocol_stage_service import (
    PIDRunProtocolStageService,
)
from libfb.py.testutil import data_provider, MagicMock


class TestPIDRunProtocolStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, mock_onedocker_service, mock_storage_service) -> None:
        self.mock_onedocker_svc = mock_onedocker_service
        self.mock_storage_svc = mock_storage_service
        self.test_num_containers = 1
        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.server_ips = [f"192.0.2.{i}" for i in range(self.test_num_containers)]
        self.input_path = "in"
        self.output_path = "out"
        self.pc_instance_id = "test_instance_123"
        self.port = 15200
        self.use_row_numbers = True

    @data_provider(
        lambda: (
            itertools.product(
                [PrivateComputationRole.PUBLISHER, PrivateComputationRole.PARTNER],
                [True, False],
            )
        )
    )
    @to_sync
    async def test_pid_run_protocol_stage(
        self, pc_role: PrivateComputationRole, multikey_enabled: bool
    ) -> None:
        protocol = (
            PIDProtocol.UNION_PID_MULTIKEY
            if self.test_num_containers == 1 and multikey_enabled
            else PIDProtocol.UNION_PID
        )
        pc_instance = self.create_sample_pc_instance(pc_role)
        stage_svc = PIDRunProtocolStageService(
            storage_svc=self.mock_storage_svc,
            onedocker_svc=self.mock_onedocker_svc,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            multikey_enabled=multikey_enabled,
        )
        containers = [
            self.create_container_instance(i) for i in range(self.test_num_containers)
        ]
        self.mock_onedocker_svc.start_containers = MagicMock(return_value=containers)
        self.mock_onedocker_svc.wait_for_pending_containers = AsyncMock(
            return_value=containers
        )
        updated_pc_instance = await stage_svc.run_async(
            pc_instance=pc_instance, server_ips=self.server_ips
        )

        binary_name = PIDRunProtocolBinaryService.get_binary_name(protocol, pc_role)
        binary_config = self.onedocker_binary_config_map[binary_name]
        env_vars = {ONEDOCKER_REPOSITORY_PATH: binary_config.repository_path}
        args_str_expect = self.get_args_expect(pc_role, protocol, self.use_row_numbers)
        # test the start_containers is called with expected parameters
        self.mock_onedocker_svc.start_containers.assert_called_with(
            package_name=binary_name,
            version=binary_config.binary_version,
            cmd_args_list=args_str_expect,
            timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
            env_vars=env_vars,
        )
        # test the return value is as expected
        self.assertEqual(
            len(updated_pc_instance.infra_config.instances),
            self.test_num_containers,
            "Failed to add the StageStageInstance into pc_instance",
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
            "Appended StageStageInstance is not as expected",
        )

    def test_stop_service(self):
        num_containers = 2
        pc_instance = self.create_sample_pc_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED
        )
        subset_data = [
            ("PID_PREPARE", False),
            ("ID_MATCH", False),
            ("ID_MATCH", True),
        ]
        for (test_state_name, stopped_containers) in subset_data:
            with self.subTest(
                test_state_name=test_state_name,
                stopped_containers=stopped_containers,
            ):
                self.mock_onedocker_svc.reset_mock()
                self.mock_storage_svc.reset_mock()
                # prepare for stop_service()
                input_pc_instance = copy.deepcopy(pc_instance)
                stage_svc = PIDRunProtocolStageService(
                    storage_svc=self.mock_storage_svc,
                    onedocker_svc=self.mock_onedocker_svc,
                    onedocker_binary_config_map=self.onedocker_binary_config_map,
                )

                containers = [
                    self.create_container_instance(i) for i in range(num_containers)
                ]
                stage_stage = StageStateInstance(
                    pc_instance.infra_config.instance_id,
                    test_state_name,
                    containers=containers,
                )
                input_pc_instance.infra_config.instances.append(stage_stage)
                if stopped_containers:
                    self.mock_onedocker_svc.stop_containers = MagicMock(
                        return_value=[None, None]
                    )
                else:
                    self.mock_onedocker_svc.stop_containers = MagicMock(
                        return_value=[None, "Oops"]
                    )
                # test if the function works as expected
                if test_state_name != "ID_MATCH":
                    with self.assertRaises(AssertionError):
                        stage_svc.stop_service(input_pc_instance)
                elif not stopped_containers:
                    with self.assertRaises(RuntimeError):
                        stage_svc.stop_service(input_pc_instance)
                    self.mock_onedocker_svc.stop_containers.assert_called_with(
                        ["test_container_instance_0", "test_container_instance_1"]
                    )
                else:
                    stage_svc.stop_service(input_pc_instance)
                    self.mock_onedocker_svc.stop_containers.assert_called_with(
                        ["test_container_instance_0", "test_container_instance_1"]
                    )

    def create_sample_pc_instance(
        self,
        pc_role: PrivateComputationRole = PrivateComputationRole.PARTNER,
        status: PrivateComputationInstanceStatus = PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED,
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.pc_instance_id,
            role=pc_role,
            status=status,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=self.test_num_containers,
        )
        common_product_config: CommonProductConfig = CommonProductConfig(
            input_path=self.input_path,
            output_dir=self.output_path,
        )
        product_config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
            pid_use_row_numbers=True,
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

    def get_args_expect(
        self,
        pc_role: PrivateComputationRole,
        protocol: PIDProtocol,
        use_row_numbers: bool,
    ) -> List[str]:
        arg_ls = []
        if (
            pc_role is PrivateComputationRole.PUBLISHER
            and protocol is PIDProtocol.UNION_PID
        ):
            arg_ls.append(
                "--host 0.0.0.0:15200 --input out/test_instance_123_out_dir/pid_stage/out.csv_publisher_prepared_0 --output out/test_instance_123_out_dir/pid_stage/out.csv_publisher_pid_matched_0 --metric-path out/test_instance_123_out_dir/pid_stage/out.csv_publisher_pid_matched_0_metrics --no-tls --use-row-numbers"
            )
        elif (
            pc_role is PrivateComputationRole.PUBLISHER
            and protocol is PIDProtocol.UNION_PID_MULTIKEY
        ):
            arg_ls.append(
                "--host 0.0.0.0:15200 --input out/test_instance_123_out_dir/pid_stage/out.csv_publisher_prepared_0 --output out/test_instance_123_out_dir/pid_stage/out.csv_publisher_pid_matched_0 --metric-path out/test_instance_123_out_dir/pid_stage/out.csv_publisher_pid_matched_0_metrics --no-tls"
            )
        elif (
            pc_role is PrivateComputationRole.PARTNER
            and protocol is PIDProtocol.UNION_PID
        ):
            arg_ls.append(
                "--company http://192.0.2.0:15200 --input out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_prepared_0 --output out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_pid_matched_0 --no-tls --use-row-numbers"
            )
        elif (
            pc_role is PrivateComputationRole.PARTNER
            and protocol is PIDProtocol.UNION_PID_MULTIKEY
        ):
            arg_ls.append(
                "--company http://192.0.2.0:15200 --input out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_prepared_0 --output out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_pid_matched_0 --no-tls"
            )
        return arg_ls
