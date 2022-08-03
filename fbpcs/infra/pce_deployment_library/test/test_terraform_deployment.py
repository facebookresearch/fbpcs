#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from subprocess import PIPE, Popen
from typing import Any, Dict, Type

from fbpcs.infra.pce_deployment_library.deploy_library.models import (
    FlaggedOption,
    NotFlaggedOption,
    RunCommandResult,
    TerraformOptionFlag,
)

from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_deployment import (
    TerraformDeployment,
)


class TestTerraformDeployment(unittest.TestCase):
    def setUp(self) -> None:
        self.terraform_deployment = TerraformDeployment()

    def test_run_command(self) -> None:
        with self.subTest("basicCaptureTrue"):
            command = "echo Hello World!\n"
            capture_output = True
            test_obj = Popen(["echo", "Hello World!"], stdout=PIPE)
            test_stdout, test_error = test_obj.communicate()
            test_return_code = test_obj.returncode
            test_command_return = RunCommandResult(
                return_code=test_return_code,
                output=test_stdout.decode("utf-8"),
                error=test_error if test_error else "",
            )
            func_ret = self.terraform_deployment.run_command(command=command)
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

        with self.subTest("basicCaptureFalse"):
            command = "echo Hello World!\n"
            capture_output = False

            test_obj = Popen(["echo", "Hello World!"])
            test_stdout, test_error = test_obj.communicate()
            test_return_code = test_obj.returncode
            test_command_return = RunCommandResult(
                return_code=test_return_code,
                output=test_stdout,
                error=test_stdout,
            )
            kwargs: Dict[str, Any] = {"capture_output": capture_output}
            func_ret = self.terraform_deployment.run_command(command=command, **kwargs)
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

        with self.subTest("TestStdErrWithCaptureOutput"):
            command = "cp"
            capture_output = True

            func_ret = self.terraform_deployment.run_command(command=command)
            test_command_return = RunCommandResult(
                return_code=1,
                output="",
                error="cp: missing file operand\nTry 'cp --help' for more information.\n",
            )
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

        with self.subTest("TestStdErrWithoutCaptureOutput"):
            command = "cp"
            capture_output = False
            kwargs: Dict[str, Any] = {"capture_output": capture_output}

            func_ret = self.terraform_deployment.run_command(command=command, **kwargs)
            test_command_return = RunCommandResult(
                return_code=1,
                output=None,
                error=None,
            )
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

    def test_terraform_init(self) -> None:
        kwargs: Dict[str, Any] = {"dry_run": True}
        with self.subTest("BackendConig"):
            backend_config = {
                "region": "fake_region",
                "access_key": "fake_access_key",
            }
            expected_command = 'terraform init -input=false -dry-run=true -backend-config "region=fake_region" -backend-config "access_key=fake_access_key" -reconfigure'
            expected_value = RunCommandResult(
                return_code=0, output=f"Dry run command: {expected_command}", error=""
            )
            return_value = self.terraform_deployment.terraform_init(
                backend_config=backend_config, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("BackendConigWhiteSpaces"):
            backend_config = {
                "region": "fake_region ",
                "access_key": "fake_access_key ",
            }
            expected_command = 'terraform init -input=false -dry-run=true -backend-config "region=fake_region " -backend-config "access_key=fake_access_key " -reconfigure'
            expected_value = RunCommandResult(
                return_code=0, output=f"Dry run command: {expected_command}", error=""
            )
            return_value = self.terraform_deployment.terraform_init(
                backend_config=backend_config, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("UnsetReconfigureNoBackendConfig"):
            expected_command = "terraform init -input=false -dry-run=true"
            expected_value = RunCommandResult(
                return_code=0, output=f"Dry run command: {expected_command}", error=""
            )
            reconfigure: Type[TerraformOptionFlag] = NotFlaggedOption
            return_value = self.terraform_deployment.terraform_init(
                reconfigure=reconfigure, **kwargs
            )
            self.assertEquals(expected_value, return_value)

        with self.subTest("SetReconfigure"):
            expected_command = "terraform init -input=false -dry-run=true -reconfigure"
            expected_value = RunCommandResult(
                return_code=0, output=f"Dry run command: {expected_command}", error=""
            )
            reconfigure = FlaggedOption
            return_value = self.terraform_deployment.terraform_init(
                reconfigure=reconfigure, **kwargs
            )
            self.assertEquals(expected_value, return_value)

    def test_create(self) -> None:
        # T126572515
        pass

    def test_destory(self) -> None:
        # T126573127
        pass

    def test_plan(self) -> None:
        # T126574725
        pass
