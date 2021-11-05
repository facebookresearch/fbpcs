#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import logging
from typing import List, Dict

import requests
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)

URL = "https://graph.facebook.com/v11.0"
GRAPHAPI_INSTANCE_STATUSES = {
    "CREATED": PrivateComputationInstanceStatus.CREATED,
    "INSTANCE_FAILURE": PrivateComputationInstanceStatus.UNKNOWN,
    "PID_SHARD_STARTED": PrivateComputationInstanceStatus.PID_SHARD_STARTED,
    "PID_SHARD_COMPLETED": PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
    "PID_SHARD_FAILED": PrivateComputationInstanceStatus.PID_SHARD_FAILED,
    "PID_PREPARE_STARTED": PrivateComputationInstanceStatus.PID_PREPARE_STARTED,
    "PID_PREPARE_COMPLETED": PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED,
    "PID_PREPARE_FAILED": PrivateComputationInstanceStatus.PID_PREPARE_FAILED,
    "ID_MATCH_STARTED": PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
    "ID_MATCH_COMPLETED": PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
    "ID_MATCH_FAILED": PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
    "COMPUTATION_STARTED": PrivateComputationInstanceStatus.COMPUTATION_STARTED,
    "COMPUTATION_COMPLETED": PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
    "COMPUTATION_FAILED": PrivateComputationInstanceStatus.COMPUTATION_FAILED,
    "AGGREGATION_STARTED": PrivateComputationInstanceStatus.AGGREGATION_STARTED,
    "RESULT_READY": PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
    "AGGREGATION_FAILED": PrivateComputationInstanceStatus.AGGREGATION_FAILED,
    "PROCESSING_REQUEST": PrivateComputationInstanceStatus.PROCESSING_REQUEST,
    "PREPARE_DATA_STARTED": PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
    "PREPARE_DATA_COMPLETED": PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
    "PREPARE_DATA_FAILED": PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
    "TIMEOUT": PrivateComputationInstanceStatus.TIMEOUT,
}


class PLGraphAPIClient:
    """
    Private Lift Graph API related functions
    __init__ contains info about all the api end points used by Private Lift
    """

    def __init__(self, access_token: str, logger: logging.Logger) -> None:
        self.access_token = access_token
        self.logger = logger
        self.params = {"access_token": self.access_token}

    def get_instance(self, instance_id: str) -> requests.Response:
        r = requests.get(
            f"{URL}/{instance_id}",
            params=self.params,
        )
        self._check_err(r, "getting fb instance")
        return r

    def create_instance(
        self, study_id: str, breakdown_key: Dict[str, str]
    ) -> requests.Response:
        params = self.params.copy()
        params["breakdown_key"] = json.dumps(breakdown_key)
        r = requests.post(f"{URL}/{study_id}/instances", params=params)
        self._check_err(r, "creating fb instance")
        return r

    def invoke_operation(self, instance_id: str, operation: str) -> None:
        params = self.params.copy()
        params["operation"] = operation
        r = requests.post(
            f"{URL}/{instance_id}",
            params=params,
        )
        self._check_err(r, "invoking operation on fb instance")

    def get_study_data(self, study_id: str, fields: List[str]) -> requests.Response:
        params = self.params.copy()
        params["fields"] = ",".join(fields)
        r = requests.get(f"{URL}/{study_id}", params=params)
        self._check_err(r, "getting study data")
        return r

    def _check_err(self, r: requests.Response, msg: str) -> None:
        if r.status_code != 200:
            err_msg = f"Error {msg}: {r.content}"
            self.logger.error(err_msg)
            raise GraphAPIGenericException(err_msg)


class GraphAPIGenericException(RuntimeError):
    pass
