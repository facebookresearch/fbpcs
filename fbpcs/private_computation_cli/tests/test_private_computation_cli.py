#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import patch

from fbpcs.private_computation_cli import private_computation_cli as pc_cli

class TestPrivateComputationCli(TestCase):
    def setUp(self):
        # We don't actually use the config, but we need to write a file so that
        # the yaml load won't blow up in `main`
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
            json.dump({}, f)
            self.temp_filename = f.name

    def tearDown(self):
        os.unlink(self.temp_filename)

    @patch("fbpcs.private_computation_cli.private_computation_cli.create_instance")
    def test_create_instance(self, create_mock):
        # Normally such *ultra-specific* test cases against a CLI would be an
        # antipattern, but since this is our public interface, we want to be
        # very careful before making that interface change.
        argv=[
            "create_instance",
            "instance123",
            f"--config={self.temp_filename}",
            "--role=PUBLISHER",
            "--game_type=LIFT",
            "--input_path=/tmp/in",
            "--output_dir=/tmp/",
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
                 "--fail_fast",
                 "--stage_flow=PrivateComputationLocalTestStageFlow",
            ]
        )
        pc_cli.main(argv)
        create_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.id_match")
    def test_id_match(self, id_match_mock):
        argv=[
            "id_match",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        id_match_mock.assert_called_once()
        id_match_mock.reset_mock()

        argv.extend(
            [
                "--server_ips=192.168.1.1,192.168.1.2",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        id_match_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.prepare_compute_input")
    def test_prepare_compute_input(self, prepare_mock):
        argv=[
            "prepare_compute_input",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        prepare_mock.assert_called_once()
        prepare_mock.reset_mock()

        argv.extend(
            [
                "--dry_run",
                "--log_cost_to_s3",
            ]
        )
        pc_cli.main(argv)
        prepare_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.compute_metrics")
    def test_compute_metrics(self, compute_mock):
        argv=[
            "compute_metrics",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        compute_mock.assert_called_once()
        compute_mock.reset_mock()

        argv.extend(
            [
                "--server_ips=192.168.1.1,192.168.1.2",
                "--dry_run",
                "--log_cost_to_s3",
            ]
        )
        pc_cli.main(argv)
        compute_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.aggregate_shards")
    def test_aggregate_shards(self, aggregate_mock):
        argv=[
            "aggregate_shards",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        aggregate_mock.assert_called_once()
        aggregate_mock.reset_mock()

        argv.extend(
            [
                "--server_ips=192.168.1.1,192.168.1.2",
                "--dry_run",
                "--log_cost_to_s3",
            ]
        )
        pc_cli.main(argv)
        aggregate_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.validate")
    def test_validate(self, validate_mock):
        argv=[
            "validate",
            "instance123",
            f"--config={self.temp_filename}",
            "--aggregated_result_path=/tmp/aggpath",
            "--expected_result_path=/tmp/exppath",
        ]
        pc_cli.main(argv)
        validate_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_post_processing_handlers")
    def test_run_post_processing_handlers(self, run_pph_mock):
        argv=[
            "run_post_processing_handlers",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        run_pph_mock.assert_called_once()
        run_pph_mock.reset_mock()

        argv.extend(
            [
                "--aggregated_result_path=/tmp/aggpath",
                "--dry_run",
            ]
        )
        pc_cli.main(argv)
        run_pph_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_next")
    def test_run_next(self, run_next_mock):
        argv=[
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
    def test_run_stage(self, run_stage_mock, get_instance_mock):
        argv=[
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
    def test_get_instance(self, get_instance_mock):
        argv=[
            "get_instance",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_server_ips")
    def test_get_server_ips(self, get_ips_mock):
        argv=[
            "get_server_ips",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_ips_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_pid")
    def test_get_pid(self, get_pid_mock):
        argv=[
            "get_pid",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_pid_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.get_mpc")
    def test_get_mpc(self, get_mpc_mock):
        argv=[
            "get_mpc",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        get_mpc_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_instance")
    def test_run_instance(self, run_instance_mock):
        argv=[
            "run_instance",
            "instance123",
            f"--config={self.temp_filename}",
            "--input_path=/tmp/in",
            "--num_shards=456",
        ]
        pc_cli.main(argv)
        run_instance_mock.assert_called_once()
        run_instance_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
                "--legacy",
            ]
        )
        pc_cli.main(argv)
        run_instance_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_instances")
    def test_run_instances(self, run_instances_mock):
        argv=[
            "run_instances",
            "instance123,instance456",
            f"--config={self.temp_filename}",
            "--input_paths=/tmp/in1,/tmp/in2",
            "--num_shards_list=456,789",
        ]
        pc_cli.main(argv)
        run_instances_mock.assert_called_once()
        run_instances_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
                "--legacy",
            ]
        )
        pc_cli.main(argv)
        run_instances_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.run_study")
    def test_run_study(self, run_study_mock):
        argv=[
            "run_study",
            "12345",
            f"--config={self.temp_filename}",
            "--objective_ids=12,34,56,78,90",
            "--input_paths=/tmp/in1,/tmp/in2,/tmp/in3,/tmp/in4,/tmp/in5",
        ]
        pc_cli.main(argv)
        run_study_mock.assert_called_once()
        run_study_mock.reset_mock()

        argv.extend(
            [
                "--tries_per_stage=789",
                "--dry_run",
                "--legacy",
            ]
        )
        pc_cli.main(argv)
        run_study_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.cancel_current_stage")
    def test_cancel_current_stage(self, cancel_stage_mock):
        argv=[
            "cancel_current_stage",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        cancel_stage_mock.assert_called_once()

    @patch("fbpcs.private_computation_cli.private_computation_cli.print_instance")
    def test_print_instance(self, print_instance_mock):
        argv=[
            "print_instance",
            "instance123",
            f"--config={self.temp_filename}",
        ]
        pc_cli.main(argv)
        print_instance_mock.assert_called_once()
