#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import base64
import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from botocore.client import BaseClient
from fbpcs.service.workflow import WorkflowService, WorkflowStatus
from requests import Response


AUTHORIZATION_TOKEN = f"Bearer {0}"
AIRFLOW_URL = f"https://{0}/aws_mwaa/cli"
TRIGGER_SUCCESS_LOG = "externally triggered: True"
RUN_ID_ALREADY_EXISTS_LOG = "Dag Run already exists"


class MwaaReturnStatus(str, Enum):
    FAILED = "failed"
    RUNNING = "running"
    SUCCESS = "success"


# Amazon Managed Workflows for Apache Airflow (MWAA)
class MwaaWorkflowService(WorkflowService):
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
            "mwaa", region_name=region, **self.config
        )

    def start_workflow(
        self,
        workflow_conf: Dict[str, str],
        run_id: str,
        run_conf: Optional[Dict[str, str]] = None,
    ) -> str:
        """Start workflow
        Keyword arguments:
        workflow_conf - airflow configs
            env_name -- Airflow environment name
            dag_id -- DAG ID
        run_id -- Run ID
        run_conf -- workflow running configs (optional)
        """
        payload: str = "dags trigger " + workflow_conf["dag_id"] + " -r " + run_id
        if run_conf:
            payload += f" -c {0}".format(json.dumps(run_conf))

        mwaa_response: Response = self.trigger_airflow_cli(
            workflow_conf["env_name"], payload
        )
        res_stdout, res_stderr = self.parse_response_plain_result(mwaa_response)

        if TRIGGER_SUCCESS_LOG in res_stdout:
            # Trigger successful
            logging.info(f"Trigger run_id {0} successfully".format(run_id))
        elif RUN_ID_ALREADY_EXISTS_LOG in res_stderr:
            # run_id already exists
            logging.error(f"run_id {0} already exists".format(run_id))
            raise Exception(f"run_id {0} already exists".format(run_id))
        else:
            # general triggering error
            logging.error(res_stderr)
            raise Exception(res_stderr)

        return run_id

    # Note: AWS blocks airflow REST API due to security reason, so only can use cli command, and parse the result from stdout to json.
    def get_workflow_status(
        self, workflow_conf: Dict[str, str], run_id: str
    ) -> WorkflowStatus:
        """Get status of specified DAG workflow
        Keyword arguments:
        workflow_conf - airflow configs
            env_name -- Airflow environment name
            dag_id -- DAG ID
        run_id -- Run ID
        """
        payload: str = (
            "tasks states-for-dag-run "
            + workflow_conf["dag_id"]
            + " "
            + run_id
            + " -o json"
        )
        mwaa_response: Response = self.trigger_airflow_cli(
            workflow_conf["env_name"], payload
        )

        try:
            tasks = self.parse_response_json_result(mwaa_response)

            statuses: List[str] = [task["state"] for task in tasks]

            if not statuses:
                return WorkflowStatus.UNKNOWN

            if MwaaReturnStatus.FAILED in statuses:
                return WorkflowStatus.FAILED

            if (MwaaReturnStatus.RUNNING in statuses) or (
                MwaaReturnStatus.SUCCESS in statuses and None in statuses
            ):
                return WorkflowStatus.STARTED

            if MwaaReturnStatus.SUCCESS in statuses and len(set(statuses)) == 1:
                return WorkflowStatus.COMPLETED

            return WorkflowStatus.CREATED
        except Exception:
            return WorkflowStatus.UNKNOWN

    def trigger_airflow_cli(self, env_name: str, payload: str) -> Response:
        mwaa_cli_token: Dict[str, str] = self.client.create_cli_token(Name=env_name)

        headers: Dict[str, str] = {
            "Authorization": AUTHORIZATION_TOKEN.format(mwaa_cli_token["CliToken"]),
            "Content-Type": "text/plain",
        }
        url: str = AIRFLOW_URL.format(mwaa_cli_token["WebServerHostname"])
        return requests.post(url, data=payload, headers=headers)

    def parse_response_plain_result(self, response: Response) -> Tuple[str, str]:
        return base64.b64decode(response.json()["stdout"]).decode(
            "utf8"
        ), base64.b64decode(response.json()["stderr"]).decode("utf8")

    def parse_response_json_result(self, response: Response) -> List[Dict[str, str]]:
        return json.loads(base64.b64decode(response.json()["stdout"]).decode("utf8"))
