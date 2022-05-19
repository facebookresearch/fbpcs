#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import MagicMock, patch

from fbpcs.service.workflow import WorkflowStatus
from fbpcs.service.workflow_sfn import SfnWorkflowService


class TestSfnWorkflowService(unittest.TestCase):
    @patch("boto3.client")
    def test_start_workflow_without_conf(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.start_execution = MagicMock(
            return_value={"executionArn": "execution_arn"}
        )
        result = service.start_workflow(
            {"state_machine_arn": "machine_arn"}, "execution_name"
        )
        self.assertEqual(result, "execution_arn")

    @patch("boto3.client")
    def test_start_workflow_with_conf(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.start_execution = MagicMock(
            return_value={"executionArn": "execution_arn"}
        )
        result = service.start_workflow(
            {"state_machine_arn": "machine_arn"}, "execution_name", {"conf": "conf1"}
        )
        self.assertEqual(result, "execution_arn")

    @patch("boto3.client")
    def test_start_workflow_got_error(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.start_execution = MagicMock(
            return_value={"errorMessage": "Got error for execution"}
        )
        with self.assertRaises(Exception):
            service.start_workflow(
                {"state_machine_arn": "machine_arn"},
                "execution_name",
                {"conf": "conf1"},
            )

    @patch("boto3.client")
    def test_get_completed_workflow_status(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.describe_execution = MagicMock(
            return_value={"status": "SUCCEEDED"}
        )
        status = service.get_workflow_status(
            {"state_machine_arn": "machine_arn"}, "execution_arn"
        )
        self.assertEqual(status, WorkflowStatus.COMPLETED)

    @patch("boto3.client")
    def test_get_failed_workflow_status(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.describe_execution = MagicMock(return_value={"status": "FAILED"})
        status = service.get_workflow_status(
            {"state_machine_arn": "machine_arn"}, "execution_arn"
        )
        self.assertEqual(status, WorkflowStatus.FAILED)

    @patch("boto3.client")
    def test_get_unknown_workflow_status(self, MockBoto3):
        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.client.describe_execution = MagicMock(
            return_value={"errorMessage": "Got error for describing execution"}
        )
        status = service.get_workflow_status(
            {"state_machine_arn": "machine_arn"}, "execution_arn"
        )
        self.assertEqual(status, WorkflowStatus.UNKNOWN)
