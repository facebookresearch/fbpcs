#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import patch

from fbpcs.private_computation_cli import private_computation_cli as pc_cli
from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict


class TestPrivateComputationCli(TestCase):
    def setUp(self) -> None:
        self.stage_flow_list = [
            "PrivateComputationLocalTestStageFlow",
            "PrivateComputationMRStageFlow",
        ]
        # We don't actually use the config, but we need to write a file so that
        # the yaml load won't blow up in `main`
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
            json.dump({}, f)
            self.temp_filename = f.name
        # Create many temporary files for testing
        self.temp_files_paths = []
        for _ in range(5):
            with tempfile.NamedTemporaryFile(
                mode="w+", delete=False
            ) as temp_file_object:
                temp_file_object.write("Hello world!")
                self.temp_files_paths.append(temp_file_object.name)
        self.temp_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        os.unlink(self.temp_filename)
        for temp_file_path in self.temp_files_paths:
            os.unlink(temp_file_path)
        shutil.rmtree(self.temp_dir_path)

    @patch("fbpcs.private_computation_cli.private_computation_cli.create_instance")
    def test_create_instance(self, create_mock) -> None:
        # Normally such *ultra-specific* test cases against a CLI would be an
        # antipattern, but since this is our public interface, we want to be
        # very careful before making that interface change.
        # Create a temporary folder for testing
        argv = [
            "create_instance",
            "instance123",
            f"--config={self.temp_filename}",
            "--role=PUBLISHER",
            "--game_type=LIFT",
            f"--input_path={self.temp_files_paths[0]}",
            f"--output_dir={self.temp_dir_path}",
            "--num_pid_containers=111",
            "--num_mpc_containers=222",
        ]
        pc_cli.main(argv)
        create_mock.assert_called_once()
        create_mock.reset_mock()
        argv.extend(
            [
                "--attribution_rule=last_click_1d",
                "--aggregation_type=measurement",
                "--concurrency=333",
                "--num_files_per_mpc_container=444",
                "--padding_size=555",
                "--k_anonymity_threshold=666",
                "--hmac_key=bigmac",
                "--stage_flow=PrivateComputationLocalTestStageFlow",
            ]
        )
        pc_cli.main(argv)
        create_mock.assert_called_once()
        # Test with additional input paths of various formats
        additional_input_paths = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
            "https://fbpcs-github-e2e.s3.Region.amazonaws.com/lift/results/partner_expected_result.json",
            "https://s3.Region.amazonaws.com/bucket-name/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
        ]
        for additional_input_path in additional_input_paths:
            create_mock.reset_mock()
            argv = [
                "create_instance",
                "instance123",
                f"--config={self.temp_filename}",
                "--role=PUBLISHER",
                "--game_type=LIFT",
                f"--input_path={additional_input_path}",
                f"--output_dir={self.temp_dir_path}",
                "--num_pid_containers=111",
                "--num_mpc_containers=222",
            ]
            pc_cli.main(argv)
            create_mock.assert_called_once()
            create_mock.reset_mock()
            argv.extend(
                [
                    "--attribution_rule=last_click_1d",
                    "--aggregation_type=measurement",
                    "--concurrency=333",
                    "--num_files_per_mpc_container=444",
                    "--padding_size=555",
                    "--k_anonymity_threshold=666",
                    "--hmac_key=bigmac",
                    "--stage_flow=PrivateComputationLocalTestStageFlow",
                ]
            )
            pc_cli.main(argv)
            create_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.create_instance")
    def test_create_instance_withmr_stageflow(self, create_mock) -> None:
        # Normally such *ultra-specific* test cases against a CLI would be an
        # antipattern, but since this is our public interface, we want to be
        # very careful before making that interface change.
        # Create a temporary folder for testing
        argv = [
            "create_instance",
            "instance123",
            f"--config={self.temp_filename}",
            "--role=PUBLISHER",
            "--game_type=LIFT",
            f"--input_path={self.temp_files_paths[0]}",
            f"--output_dir={self.temp_dir_path}",
            "--num_pid_containers=111",
            "--num_mpc_containers=222",
        ]
        pc_cli.main(argv)
        create_mock.assert_called_once()
        create_mock.reset_mock()
        argv.extend(
            [
                "--attribution_rule=last_click_1d",
                "--aggregation_type=measurement",
                "--concurrency=333",
                "--num_files_per_mpc_container=444",
                "--padding_size=555",
                "--k_anonymity_threshold=666",
                "--hmac_key=bigmac",
                "--stage_flow=PrivateComputationMRStageFlow",
            ]
        )
        pc_cli.main(argv)
        create_mock.assert_called_once()
        # Test with additional input paths of various formats
        additional_input_paths = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
            "https://fbpcs-github-e2e.s3.Region.amazonaws.com/lift/results/partner_expected_result.json",
            "https://s3.Region.amazonaws.com/bucket-name/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
        ]
        for additional_input_path in additional_input_paths:
            create_mock.reset_mock()
            argv = [
                "create_instance",
                "instance123",
                f"--config={self.temp_filename}",
                "--role=PUBLISHER",
                "--game_type=LIFT",
                f"--input_path={additional_input_path}",
                f"--output_dir={self.temp_dir_path}",
                "--num_pid_containers=111",
                "--num_mpc_containers=222",
            ]
            pc_cli.main(argv)
            create_mock.assert_called_once()
            create_mock.reset_mock()
            argv.extend(
                [
                    "--attribution_rule=last_click_1d",
                    "--aggregation_type=measurement",
                    "--concurrency=333",
                    "--num_files_per_mpc_container=444",
                    "--padding_size=555",
                    "--k_anonymity_threshold=666",
                    "--hmac_key=bigmac",
                    "--stage_flow=PrivateComputationMRStageFlow",
                ]
            )
            pc_cli.main(argv)
            create_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_attribution")
    def test_run_attribution(self, create_mock) -> None:
        argv = [
            "run_attribution",
            "--dataset_id=43423422232",
            "--attribution_rule=last_click_1d",
            f"--input_path={self.temp_files_paths[0]}",
            "--aggregation_type=measurement",
            "--concurrency=4",
            "--num_files_per_mpc_container=4",
            f"--config={self.temp_filename}",
            "--timestamp=1646870400",
            "--k_anonymity_threshold=0",
        ]
        pc_cli.main(argv)
        create_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.validate")
    def test_validate(self, validate_mock) -> None:
        argv = [
            "validate",
            "instance123",
            f"--config={self.temp_filename}",
            "--aggregated_result_path=/tmp/aggpath",
            "--expected_result_path=/tmp/exppath",
        ]
        pc_cli.main(argv)
        validate_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_next")
    def test_run_next(self, run_next_mock) -> None:
        argv = [
            "run_next",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        run_next_mock.assert_called_once()
        run_next_mock.reset_mock()

        argv.extend(
            [
                "--server_ips=192.168.1.1,192.168.1.2",
            ]
        )
        pc_cli.main(argv)
        run_next_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_instance")
    @patch("fbpcs.private_computation_cli.private_computation_cli.run_stage")
    def test_run_stage(self, run_stage_mock, get_instance_mock) -> None:
        argv = [
            "run_stage",
            "instance123",
            "--stage=hamlet",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        run_stage_mock.assert_called_once()
        get_instance_mock.assert_called_once()
        run_stage_mock.reset_mock()
        get_instance_mock.reset_mock()

        argv.extend(
            [
                "--server_ips=192.168.1.1,192.168.1.2",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        run_stage_mock.assert_called_once()
        get_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_instance")
    def test_get_instance(self, get_instance_mock) -> None:
        argv = [
            "get_instance",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_server_ips")
    def test_get_server_ips(self, get_ips_mock) -> None:
        argv = [
            "get_server_ips",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_ips_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_pid")
    def test_get_pid(self, get_pid_mock) -> None:
        argv = [
            "get_pid",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_pid_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_mpc")
    def test_get_mpc(self, get_mpc_mock) -> None:
        argv = [
            "get_mpc",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_mpc_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_instance")
    def test_run_instance(self, run_instance_mock) -> None:
        argv = [
            "run_instance",
            "instance123",
            f"--config={self.temp_filename}",
            f"--input_path={self.temp_filename}",
            "--num_shards=456",
        ]
        pc_cli.main(argv)
        run_instance_mock.assert_called_once()
        run_instance_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        run_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_instances")
    def test_run_instances(self, run_instances_mock) -> None:
        # Test with real temporary file and folder
        argv = [
            "run_instances",
            "instance123,instance456",
            f"--config={self.temp_filename}",
            f"--input_paths={','.join(self.temp_files_paths[:2])}",
            "--num_shards_list=456,789",
        ]
        pc_cli.main(argv)
        run_instances_mock.assert_called_once()
        run_instances_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        run_instances_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_study")
    def test_run_study(self, run_study_mock) -> None:
        argv = [
            "run_study",
            "12345",
            f"--config={self.temp_filename}",
            "--objective_ids=12,34,56,78,90",
            f"--input_paths={','.join(self.temp_files_paths)},",
        ]
        pc_cli.main(argv)
        run_study_mock.assert_called_once()
        run_study_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        run_study_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.PreValidateService")
    @patch("fbpcs.private_computation_cli.private_computation_cli.logging.getLogger")
    def test_pre_validate_with_pl_args(
        self, getLoggerMock, pre_validate_service_mock
    ) -> None:
        getLoggerMock.return_value = getLoggerMock
        expected_config = ConfigYamlDict.from_file(self.temp_filename)
        argv = [
            "pre_validate",
            "12345",
            f"--config={self.temp_filename}",
            "--objective_ids=12,34,56,78,90",
            f"--input_paths={','.join(self.temp_files_paths)}",
        ]

        pc_cli.main(argv)

        pre_validate_service_mock.pre_validate.assert_called_once_with(
            config=expected_config,
            input_paths=self.temp_files_paths,
            logger=getLoggerMock,
        )

    @patch("fbpcs.private_computation_cli.private_computation_cli.PreValidateService")
    @patch("fbpcs.private_computation_cli.private_computation_cli.logging.getLogger")
    def test_pre_validate_with_pa_args(
        self, getLoggerMock, pre_validate_service_mock
    ) -> None:
        getLoggerMock.return_value = getLoggerMock
        expected_config = ConfigYamlDict.from_file(self.temp_filename)
        argv = [
            "pre_validate",
            f"--config={self.temp_filename}",
            "--dataset_id=123",
            f"--input_path={self.temp_files_paths[0]}",
            "--timestamp=1651847976",
            "--attribution_rule=last_click_1d",
            "--aggregation_type=measurement",
            "--concurrency=1",
            "--num_files_per_mpc_container=1",
            "--k_anonymity_threshold=10",
        ]

        pc_cli.main(argv)

        pre_validate_service_mock.pre_validate.assert_called_once_with(
            config=expected_config,
            input_paths=[self.temp_files_paths[0]],
            logger=getLoggerMock,
        )

    @patch("fbpcs.private_computation_cli.private_computation_cli.PreValidateService")
    @patch("fbpcs.private_computation_cli.private_computation_cli.logging.getLogger")
    def test_pre_validate_with_minimal_input_path_args(
        self, getLoggerMock, pre_validate_service_mock
    ) -> None:
        getLoggerMock.return_value = getLoggerMock
        expected_config = ConfigYamlDict.from_file(self.temp_filename)
        argv = [
            "pre_validate",
            f"--config={self.temp_filename}",
            f"--input_path={self.temp_files_paths[0]}",
        ]

        pc_cli.main(argv)

        pre_validate_service_mock.pre_validate.assert_called_once_with(
            config=expected_config,
            input_paths=[self.temp_files_paths[0]],
            logger=getLoggerMock,
        )

    @patch("fbpcs.private_computation_cli.private_computation_cli.PreValidateService")
    @patch("fbpcs.private_computation_cli.private_computation_cli.logging.getLogger")
    def test_pre_validate_with_minimal_input_paths_args(
        self, getLoggerMock, pre_validate_service_mock
    ) -> None:
        getLoggerMock.return_value = getLoggerMock
        expected_config = ConfigYamlDict.from_file(self.temp_filename)
        argv = [
            "pre_validate",
            f"--config={self.temp_filename}",
            f"--input_paths={','.join(self.temp_files_paths)}",
        ]

        pc_cli.main(argv)

        pre_validate_service_mock.pre_validate.assert_called_once_with(
            config=expected_config,
            input_paths=self.temp_files_paths,
            logger=getLoggerMock,
        )

    @patch("fbpcs.private_computation_cli.private_computation_cli.cancel_current_stage")
    def test_cancel_current_stage(self, cancel_stage_mock) -> None:
        argv = [
            "cancel_current_stage",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        cancel_stage_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.print_instance")
    def test_print_instance(self, print_instance_mock) -> None:
        argv = [
            "print_instance",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        print_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.print_current_status")
    def test_print_current_status(self, print_current_status_mock) -> None:
        argv = [
            "print_current_status",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        print_current_status_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.print_log_urls")
    def test_print_log_urls(self, print_log_urls_mock) -> None:
        argv = [
            "print_log_urls",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        print_log_urls_mock.assert_called_once()

    @patch(
        "fbpcs.private_computation_cli.private_computation_cli.get_attribution_dataset_info"
    )
    def test_get_attribution_dataset_info(
        self, get_attribution_dataset_info_mock
    ) -> None:
        argv = [
            "get_attribution_dataset_info",
            "--dataset_id=dataset123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_attribution_dataset_info_mock.assert_called_once()
