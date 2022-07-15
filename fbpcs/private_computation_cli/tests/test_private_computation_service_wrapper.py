#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase
from unittest.mock import ANY, call, MagicMock, patch

from fbpcp.repository.mpc_game_repository import MPCGameRepository
from fbpcp.repository.mpc_instance import MPCInstanceRepository
from fbpcp.service.container import ContainerService
from fbpcp.service.mpc_game import MPCGameService
from fbpcp.service.storage import StorageService
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType

from fbpcs.private_computation.entity.pcs_tier import PCSTier

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)

from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    _build_private_computation_service,
    cancel_current_stage,
    create_instance,
    get_instance,
    get_tier,
    run_next,
    run_stage,
    update_input_path,
    validate,
)


class TestPrivateComputationServiceWrapper(TestCase):
    test_instance_id = "test_instance_id"
    config = {
        "private_computation": {
            "dependency": {
                "PrivateComputationInstanceRepository": None,
                "ContainerService": None,
                "OneDockerServiceConfig": {
                    "constructor": {
                        "task_definition": "__task_definition__",
                    }
                },
                "OneDockerBinaryConfig": {
                    "default": {
                        "constructor": {
                            "tmp_directory": "/tmp",
                            "binary_version": "latest",
                        }
                    }
                },
                "StorageService": {
                    "constructor": {
                        "region": "__region__",
                    }
                },
                "ValidationConfig": {
                    "is_validating": False,
                },
            }
        },
        "mpc": {
            "dependency": {
                "MPCGameService": {
                    "class": "__MPCGameServiceClass__",
                    "dependency": {
                        "PrivateComputationGameRepository": {
                            "class": "__PrivateComputationGameRepositoryClass__",
                        },
                    },
                },
                "MPCInstanceRepository": None,
            }
        },
        "pid": {
            "dependency": {
                "PIDInstanceRepository": None,
            }
        },
    }

    def setUp(self) -> None:
        self.mock_pcs = MagicMock(autospec=PrivateComputationService)

    @patch("fbpcs.utils.config_yaml.reflect.get_class")
    @patch("fbpcs.utils.config_yaml.reflect.get_instance")
    def test_build_pcs(
        self,
        mock_reflect_get_instance,
        mock_reflect_get_class,
    ) -> None:
        _build_private_computation_service(
            pc_config=self.config["private_computation"],
            mpc_config=self.config["mpc"],
            pid_config=self.config["pid"],
            pph_config=self.config.get("post_processing_handlers", {}),
            pid_pph_config=self.config.get("pid_post_processing_handlers", {}),
        )

        self.assertEqual(mock_reflect_get_instance.call_count, 5)
        calls = [
            call(None, PrivateComputationInstanceRepository),
            call(None, ContainerService),
            call({"constructor": {"region": "__region__"}}, StorageService),
            call(None, MPCInstanceRepository),
            call(
                {"class": "__PrivateComputationGameRepositoryClass__"},
                MPCGameRepository,
            ),
        ]
        mock_reflect_get_instance.assert_has_calls(calls, any_order=True)
        mock_reflect_get_class.assert_called_once_with(
            "__MPCGameServiceClass__", MPCGameService
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_create_instance(
        self,
        mock_build_pcs,
    ) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        create_instance(
            config=self.config,
            instance_id=self.test_instance_id,
            role=PrivateComputationRole.PUBLISHER,
            game_type=PrivateComputationGameType.LIFT,
            logger=MagicMock(),
            input_path="input_path",
            output_dir="output_path",
            num_pid_containers=1,
            num_mpc_containers=1,
        )
        self.mock_pcs.create_instance.assert_called_once_with(
            instance_id=self.test_instance_id,
            role=PrivateComputationRole.PUBLISHER,
            game_type=PrivateComputationGameType.LIFT,
            input_path="input_path",
            output_dir="output_path",
            num_pid_containers=1,
            num_mpc_containers=1,
            concurrency=None,
            attribution_rule=None,
            aggregation_type=None,
            tier=ANY,
            num_files_per_mpc_container=None,
            hmac_key=None,
            padding_size=None,
            k_anonymity_threshold=None,
            stage_flow_cls=None,
            pid_configs={"dependency": {"PIDInstanceRepository": None}},
            result_visibility=None,
            pcs_features=None,
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_validate(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        validate(
            self.config,
            self.test_instance_id,
            MagicMock(),
            "expected_result_path",
        )
        self.mock_pcs.validate_metrics.assert_called_once_with(
            instance_id=self.test_instance_id,
            aggregated_result_path=None,
            expected_result_path="expected_result_path",
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_run_next(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        run_next(
            config=self.config,
            instance_id=self.test_instance_id,
            logger=MagicMock(),
        )
        self.mock_pcs.update_instance.assert_called_once_with(self.test_instance_id)
        self.mock_pcs.run_next.assert_called_once_with(
            instance_id=self.test_instance_id, server_ips=None
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_run_stage(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        run_stage(
            config=self.config,
            instance_id=self.test_instance_id,
            stage=PrivateComputationStageFlow.ID_MATCH,
            logger=MagicMock(),
        )
        self.mock_pcs.update_instance.assert_called_once_with(self.test_instance_id)
        self.mock_pcs.run_stage.assert_called_once_with(
            instance_id=self.test_instance_id,
            stage=PrivateComputationStageFlow.ID_MATCH,
            server_ips=None,
            dry_run=False,
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_update_input_path(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        update_input_path(
            config=self.config,
            instance_id=self.test_instance_id,
            input_path="input_path",
            logger=MagicMock(),
        )
        self.mock_pcs.update_input_path.assert_called_once_with(
            self.test_instance_id, "input_path"
        )

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_get_instance(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        get_instance(
            config=self.config,
            instance_id=self.test_instance_id,
            logger=MagicMock(),
        )
        self.mock_pcs.get_instance.assert_called_once_with(self.test_instance_id)

    @patch(
        "fbpcs.private_computation_cli.private_computation_service_wrapper._build_private_computation_service"
    )
    def test_cancel_current_stage(self, mock_build_pcs) -> None:
        mock_build_pcs.return_value = self.mock_pcs
        cancel_current_stage(
            config=self.config,
            instance_id=self.test_instance_id,
            logger=MagicMock(),
        )
        self.mock_pcs.cancel_current_stage.assert_called_once_with(
            instance_id=self.test_instance_id
        )

    def test_get_tier(self) -> None:
        pcs_tier = get_tier(self.config)
        # passing config with 'latest' version
        self.assertEqual(pcs_tier, PCSTier.PROD)
