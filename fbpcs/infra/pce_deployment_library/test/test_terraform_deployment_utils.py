# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from typing import Any, Dict

from fbpcs.infra.pce_deployment_library.deploy_library.models import FlaggedOption

from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_deployment_utils import (
    TerraformDeploymentUtils,
)


class TestTerraformDeploymentUtils(unittest.TestCase):
    def setUp(self) -> None:
        self.terraform_deployment_utils = TerraformDeploymentUtils()

    def test_get_default_options(self) -> None:
        # T125643751
        pass

    def test_get_command_list(self) -> None:
        command: str = "terraform apply"
        with self.subTest("OptionTypeDict"):
            kwargs: Dict[str, Any] = {
                "backend-config": {
                    "region": "fake_region",
                    "access_key": "fake_access_key",
                }
            }
            expected_value = [
                "terraform",
                "apply",
                "-backend-config region=fake_region",
                "-backend-config access_key=fake_access_key",
            ]
            return_value = self.terraform_deployment_utils.get_command_list(
                command, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("OptionTypeList"):
            kwargs: Dict[str, Any] = {"target": ["fake_region", "fake_access_key"]}
            expected_value = [
                "terraform",
                "apply",
                "-target=fake_region",
                "-target=fake_access_key",
            ]
            return_value = self.terraform_deployment_utils.get_command_list(
                command, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("OptionTypeBool"):
            kwargs: Dict[str, Any] = {"input": "false"}
            expected_value = ["terraform", "apply", "-input=false"]
            return_value = self.terraform_deployment_utils.get_command_list(
                command, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("OptionTypeFlaggedOption"):
            kwargs: Dict[str, Any] = {"reconfigure": FlaggedOption}
            expected_value = ["terraform", "apply", "-reconfigure"]
            return_value = self.terraform_deployment_utils.get_command_list(
                command, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("OptionTypeDictWithArgs"):
            kwargs: Dict[str, Any] = {
                "backend-config": {
                    "region": "fake_region",
                    "access_key": "fake_access_key",
                }
            }
            args = ("test_test",)
            expected_value = [
                "terraform",
                "apply",
                "-backend-config region=fake_region",
                "-backend-config access_key=fake_access_key",
                "test_test",
            ]
            return_value = self.terraform_deployment_utils.get_command_list(
                command, *args, **kwargs
            )
            self.assertEquals(expected_value, return_value)

    def test_add_dict_options(self) -> None:
        pass

    def test_add_list_options(self) -> None:
        pass

    def test_add_bool_options(self) -> None:
        pass

    def test_add_flagged_option(self) -> None:
        pass

    def test_add_other_options(self) -> None:
        pass
