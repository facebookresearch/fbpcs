# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from pathlib import Path

from fbpcp.util import yaml
from fbpcs.private_computation.entity.exceptions import CannotFindDependencyError
from fbpcs.private_computation.entity.pc_infra_config import (
    PrivateComputationInfraConfig,
)


class TestPrivateComputationInfraConfig(unittest.TestCase):
    def setUp(self) -> None:
        # call Path.revolve() to make the path absolute
        self.input_dir = Path(__file__).resolve().parent / "test_resources" / "configs"

    def test_build_full_config(self) -> None:
        config_path = self.input_dir / "expected_mini_config.yml"
        yml_config = yaml.load(config_path)

        actual_config = PrivateComputationInfraConfig.build_full_config(yml_config)

        self.assertEqual(yml_config, actual_config)

    def test_build_full_config_mini(self) -> None:
        expected_mini_config_path = self.input_dir / "expected_mini_config.yml"
        expected_mini_config = yaml.load(expected_mini_config_path)

        mini_config_path = self.input_dir / "mini_config.yml"
        yml_config = yaml.load(mini_config_path)
        actual_config = PrivateComputationInfraConfig.build_full_config(yml_config)

        self.assertEqual(actual_config, expected_mini_config)

    def test_build_full_config_mini_override(self) -> None:
        expected_mini_override_config_path = (
            self.input_dir / "expected_mini_override_config.yml"
        )
        expected_mini_override_config = yaml.load(expected_mini_override_config_path)

        mini_override_config_path = self.input_dir / "mini_override_config.yml"
        yml_config = yaml.load(mini_override_config_path)
        actual_config = PrivateComputationInfraConfig.build_full_config(yml_config)

        self.assertEqual(expected_mini_override_config, actual_config)

    def test_build_full_config_mini_override_error(self) -> None:
        mini_override_error_config_path = (
            self.input_dir / "mini_override_error_config.yml"
        )
        yml_config = yaml.load(mini_override_error_config_path)

        with self.assertRaises(CannotFindDependencyError):
            PrivateComputationInfraConfig.build_full_config(yml_config)
