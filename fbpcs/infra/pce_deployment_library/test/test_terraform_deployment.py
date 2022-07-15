#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from subprocess import PIPE, Popen

from fbpcs.infra.pce_deployment_library.deploy_library.models import RunCommandResult

from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_deployment import (
    TerraformDeployment,
)


class TestTerraformDeployment(unittest.TestCase):
    def setUp(self) -> None:
        self.terraform = TerraformDeployment()

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
            func_ret = self.terraform.run_command(
                command=command, capture_output=capture_output
            )
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

            func_ret = self.terraform.run_command(
                command=command, capture_output=capture_output
            )
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

        with self.subTest("TestStdErrWithCaptureOutput"):
            command = "cp"
            capture_output = True

            func_ret = self.terraform.run_command(
                command=command, capture_output=capture_output
            )
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

            func_ret = self.terraform.run_command(
                command=command, capture_output=capture_output
            )
            test_command_return = RunCommandResult(
                return_code=1,
                output=None,
                error=None,
            )
            self.assertEqual(test_command_return.return_code, func_ret.return_code)
            self.assertEqual(test_command_return.output, func_ret.output)
            self.assertEqual(test_command_return.error, func_ret.error)

    def test_terraform_init(self) -> None:
        pass

    def test_create(self) -> None:
        pass
