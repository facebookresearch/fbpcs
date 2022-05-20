#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from enum import Enum
from typing import Any, Dict, Optional

import boto3
from botocore.client import BaseClient
from fbpcs.service.workflow import WorkflowService, WorkflowStatus


class SfnReturnStatus(str, Enum):
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    TIMED_OUT = "TIMED_OUT"
    ABORTED = "ABORTED"


# AWS Step Functions workflow
class SfnWorkflowService(WorkflowService):
    def __init__(
        self,
        region: str = "us-west-2",
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.config: Dict[str, Any] = config or {}

        if access_key_id is not None:
            self.config["aws_access_key_id"] = access_key_id

        if access_key_data is not None:
            self.config["aws_secret_access_key"] = access_key_data

        self.client: BaseClient = boto3.client(
            "stepfunctions", region_name=region, **self.config
        )

    def start_workflow(
        self,
        workflow_conf: Dict[str, str],
        run_id: str,
        run_conf: Optional[Dict[str, str]] = None,
    ) -> str:
        """Start workflow
        Keyword arguments:
        workflow_conf - step functions configs
            state_machine_arn -- state machine ARN
        run_id -- execution name
        run_conf -- workflow running configs (optional)
        """
        response = self.client.start_execution(
            stateMachineArn=workflow_conf["state_machine_arn"],
            name=run_id,
            input=json.dumps(run_conf) if run_conf else "{}",
        )

        if "executionArn" not in response:
            # Trigger execution got error
            logging.error(response["errorMessage"])
            raise Exception(response["errorMessage"])

        return response["executionArn"]

    def get_workflow_status(
        self, workflow_conf: Dict[str, str], run_id: str
    ) -> WorkflowStatus:
        """Get status of specified step functions workflow
        Keyword arguments:
        workflow_conf - step functions configs
        run_id -- Execution Arn
        """
        response = self.client.describe_execution(executionArn=run_id)

        try:
            status = response["status"]

            if status == SfnReturnStatus.RUNNING:
                return WorkflowStatus.STARTED

            if status == SfnReturnStatus.SUCCEEDED:
                return WorkflowStatus.COMPLETED

            if status in (
                SfnReturnStatus.FAILED,
                SfnReturnStatus.TIMED_OUT,
                SfnReturnStatus.ABORTED,
            ):
                return WorkflowStatus.FAILED

            return WorkflowStatus.UNKNOWN
        except Exception:
            # Describe execution got error
            logging.error(response["errorMessage"])
            return WorkflowStatus.UNKNOWN
