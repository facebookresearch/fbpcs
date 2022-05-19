#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import MagicMock, patch

from fbpcs.service.workflow import WorkflowStatus
from fbpcs.service.workflow_mwaa import MwaaWorkflowService


class TestMwaaWorkflowService(unittest.TestCase):
    @patch("boto3.client")
    @patch("requests.post")
    def test_start_workflow_without_conf(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_plain_result = MagicMock(
            return_value=("test_run_id, externally triggered: True", "test stderr")
        )
        service.start_workflow(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")

    @patch("boto3.client")
    @patch("requests.post")
    def test_start_workflow_with_conf(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_plain_result = MagicMock(
            return_value=("test_run_id, externally triggered: True", "test stderr")
        )
        service.start_workflow(
            {"env_name": "test_env", "dag_id": "test_dag"},
            "test_run_id",
            {"conf1": "test_conf"},
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")

    @patch("boto3.client")
    @patch("requests.post")
    def test_start_workflow_with_existing_run_id(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_plain_result = MagicMock(
            return_value=("test stdout", "Dag Run already exists for test_dag")
        )
        with self.assertRaises(Exception):
            service.start_workflow(
                {"env_name": "test_env", "dag_id": "test_dag"},
                "test_run_id",
                {"conf1": "test_conf"},
            )

    @patch("boto3.client")
    @patch("requests.post")
    def test_start_workflow_with_invalid_dag_id(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_plain_result = MagicMock(
            return_value=("test stdout", "Dag id test_dag not found in DagModel")
        )
        with self.assertRaises(Exception):
            service.start_workflow(
                {"env_name": "test_env", "dag_id": "test_dag"},
                "test_run_id",
                {"conf1": "test_conf"},
            )

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_failed_workflow_status(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": "success"},
                {"dag_id": "test_dag", "task_id": "task2", "state": "failed"},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.FAILED)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_started_workflow_status_case_1(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": "success"},
                {"dag_id": "test_dag", "task_id": "task2", "state": "running"},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.STARTED)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_started_workflow_status_case_2(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": "success"},
                {"dag_id": "test_dag", "task_id": "task2", "state": None},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.STARTED)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_completed_workflow_status(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": "success"},
                {"dag_id": "test_dag", "task_id": "task2", "state": "success"},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.COMPLETED)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_created_workflow_status(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": None},
                {"dag_id": "test_dag", "task_id": "task2", "state": None},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.CREATED)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_unknown_workflow_status_case1(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(
            return_value=[
                {"dag_id": "test_dag", "task_id": "task1", "state": "success"},
                {"dag_id": "test_dag", "task_id": "task2"},
            ]
        )
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.UNKNOWN)

    @patch("boto3.client")
    @patch("requests.post")
    def test_get_unknown_workflow_status_case2(self, MockPost, MockBoto3):
        service = MwaaWorkflowService("us-west-2", "access_key", "access_data")
        service.client.create_cli_token = MagicMock(
            return_value={"CliToken": "cli_token", "WebServerHostname": "host_name"}
        )
        service.parse_response_json_result = MagicMock(return_value=[])
        status = service.get_workflow_status(
            {"env_name": "test_env", "dag_id": "test_dag"}, "test_run_id"
        )
        service.client.create_cli_token.assert_called_with(Name="test_env")
        self.assertEqual(status, WorkflowStatus.UNKNOWN)
