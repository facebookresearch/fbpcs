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

    def test_id_match(self):
        pass

    def test_prepare_compute_input(self):
        pass

    def test_compute_metrics(self):
        pass

    def test_aggregate_shards(self):
        pass

    def test_validate(self):
        pass

    def test_run_post_processing_handlers(self):
        pass

    def test_run_next(self):
        pass

    def test_run_stage(self):
        pass

    def test_get_instance(self):
        pass

    def test_get_server_ips(self):
        pass

    def test_get_pid(self):
        pass

    def test_get_mpc(self):
        pass

    def test_run_instance(self):
        pass

    def test_run_instances(self):
        pass

    def test_run_study(self):
        pass

    def test_cancel_current_stage(self):
        pass

    def test_print_instance(self):
        pass
