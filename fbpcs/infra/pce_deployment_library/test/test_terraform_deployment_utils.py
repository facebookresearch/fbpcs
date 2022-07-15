# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

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
        # T125643785
        pass

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
