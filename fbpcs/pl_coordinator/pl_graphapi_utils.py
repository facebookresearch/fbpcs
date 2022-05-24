#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import logging
import os
from typing import Any, Dict, List

import requests
from fbpcs.pl_coordinator.constants import FBPCS_GRAPH_API_TOKEN
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict
from fbpcs.utils.config_yaml.exceptions import ConfigYamlBaseException

URL = "https://graph.facebook.com/v13.0"
GRAPHAPI_INSTANCE_STATUSES: Dict[str, PrivateComputationInstanceStatus] = {
    "CREATED": PrivateComputationInstanceStatus.CREATED,
    "INPUT_DATA_VALIDATION_STARTED": PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_STARTED,
    "INPUT_DATA_VALIDATION_COMPLETED": PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED,
    "INPUT_DATA_VALIDATION_FAILED": PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_FAILED,
    "PC_PRE_VALIDATION_STARTED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
    "PC_PRE_VALIDATION_COMPLETED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
    "PC_PRE_VALIDATION_FAILED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
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
    "ID_MATCHING_POST_PROCESS_STARTED": PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_STARTED,
    "ID_MATCHING_POST_PROCESS_COMPLETED": PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_COMPLETED,
    "ID_MATCHING_POST_PROCESS_FAILED": PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_FAILED,
    "COMPUTATION_STARTED": PrivateComputationInstanceStatus.COMPUTATION_STARTED,
    "COMPUTATION_COMPLETED": PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
    "COMPUTATION_FAILED": PrivateComputationInstanceStatus.COMPUTATION_FAILED,
    "DECOUPLED_ATTRIBUTION_STARTED": PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_STARTED,
    "DECOUPLED_ATTRIBUTION_COMPLETED": PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_COMPLETED,
    "DECOUPLED_ATTRIBUTION_FAILED": PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_FAILED,
    "DECOUPLED_AGGREGATION_STARTED": PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_STARTED,
    "DECOUPLED_AGGREGATION_COMPLETED": PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_COMPLETED,
    "DECOUPLED_AGGREGATION_FAILED": PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_FAILED,
    "PCF2_LIFT_STARTED": PrivateComputationInstanceStatus.PCF2_LIFT_STARTED,
    "PCF2_LIFT_COMPLETED": PrivateComputationInstanceStatus.PCF2_LIFT_COMPLETED,
    "PCF2_LIFT_FAILED": PrivateComputationInstanceStatus.PCF2_LIFT_FAILED,
    "PCF2_ATTRIBUTION_STARTED": PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_STARTED,
    "PCF2_ATTRIBUTION_COMPLETED": PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_COMPLETED,
    "PCF2_ATTRIBUTION_FAILED": PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_FAILED,
    "PCF2_AGGREGATION_STARTED": PrivateComputationInstanceStatus.PCF2_AGGREGATION_STARTED,
    "PCF2_AGGREGATION_COMPLETED": PrivateComputationInstanceStatus.PCF2_AGGREGATION_COMPLETED,
    "PCF2_AGGREGATION_FAILED": PrivateComputationInstanceStatus.PCF2_AGGREGATION_FAILED,
    "AGGREGATION_STARTED": PrivateComputationInstanceStatus.AGGREGATION_STARTED,
    "RESULT_READY": PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
    "AGGREGATION_FAILED": PrivateComputationInstanceStatus.AGGREGATION_FAILED,
    "PROCESSING_REQUEST": PrivateComputationInstanceStatus.PROCESSING_REQUEST,
    "PREPARE_DATA_STARTED": PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
    "PREPARE_DATA_COMPLETED": PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
    "PREPARE_DATA_FAILED": PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
    "ID_SPINE_COMBINER_STARTED": PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
    "ID_SPINE_COMBINER_COMPLETED": PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
    "ID_SPINE_COMBINER_FAILED": PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
    "RESHARD_STARTED": PrivateComputationInstanceStatus.RESHARD_STARTED,
    "RESHARD_COMPLETED": PrivateComputationInstanceStatus.RESHARD_COMPLETED,
    "RESHARD_FAILED": PrivateComputationInstanceStatus.RESHARD_FAILED,
    "TIMEOUT": PrivateComputationInstanceStatus.TIMEOUT,
    "PID_MR_STARTED": PrivateComputationInstanceStatus.PID_MR_STARTED,
    "PID_MR_COMPLETED": PrivateComputationInstanceStatus.PID_MR_COMPLETED,
    "PID_MR_FAILED": PrivateComputationInstanceStatus.PID_MR_FAILED,
}


# TODO(T116610959): rename pl_graph_api_utils.py and its entities to PC equivalents
class PLGraphAPIClient:
    """
    Private Lift Graph API related functions
    __init__ contains info about all the api end points used by Private Lift
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger) -> None:
        self.logger = logger
        self.access_token = self._get_graph_api_token(config)
        self.params = {"access_token": self.access_token}

    def _get_graph_api_token(self, config: Dict[str, Any]) -> str:
        f"""Get graph API token from config.yml or the {FBPCS_GRAPH_API_TOKEN} env var

        Arguments:
            config: dictionary representation of config.yml file

        Returns:
            the graph api token

        Raises:
            GraphAPITokenNotFound: graph api token not in config.yml and not in env var
        """
        try:
            if not isinstance(config, ConfigYamlDict):
                config = ConfigYamlDict.from_dict(config)
            self.logger.info("attempting to read graph api token from config.yml file")
            token = config["graphapi"]["access_token"]
            self.logger.info("successfuly read graph api token from config.yml file")
        except ConfigYamlBaseException:
            self.logger.info(
                f"attempting to read graph api token from {FBPCS_GRAPH_API_TOKEN} env var"
            )
            token = os.getenv(FBPCS_GRAPH_API_TOKEN)
            if not token:
                no_token_exception = GraphAPITokenNotFound.make_error()
                self.logger.exception(no_token_exception)
                raise no_token_exception from None
            self.logger.info(
                f"successfully read graph api token from {FBPCS_GRAPH_API_TOKEN} env var"
            )
        return token

    def get_instance(self, instance_id: str) -> requests.Response:
        r = requests.get(
            f"{URL}/{instance_id}",
            params=self.params,
        )
        self._check_err(r, "getting fb instance")
        return r

    # TODO rename to create_pl_instance since we now have a create_pa_instance function
    def create_instance(
        self, study_id: str, breakdown_key: Dict[str, str]
    ) -> requests.Response:
        params = self.params.copy()
        params["breakdown_key"] = json.dumps(breakdown_key)
        r = requests.post(f"{URL}/{study_id}/instances", params=params)
        self._check_err(r, "creating fb instance")
        return r

    def create_pa_instance(
        self,
        dataset_id: str,
        timestamp: int,
        attribution_rule: str,
        num_containers: int,
    ) -> requests.Response:
        params = self.params.copy()
        params["attribution_rule"] = attribution_rule
        params["timestamp"] = timestamp
        r = requests.post(f"{URL}/{dataset_id}/instance", params=params)
        self._check_err(r, "creating fb pa instance")
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

    def get_attribution_dataset_info(
        self, dataset_id: str, fields: List[str]
    ) -> requests.Response:
        params = self.params.copy()
        params["fields"] = ",".join(fields)
        r = requests.get(f"{URL}/{dataset_id}", params=params)
        self._check_err(r, "getting dataset information")
        return r

    def get_existing_pa_instances(self, dataset_id: str) -> requests.Response:
        params = self.params.copy()
        r = requests.get(f"{URL}/{dataset_id}/instances", params=params)
        self._check_err(r, "getting attribution instances tied to the dataset")
        return r

    def _check_err(self, r: requests.Response, msg: str) -> None:
        if r.status_code != 200:
            err_msg = f"Error {msg}: {r.content}"
            self.logger.error(err_msg)
            raise GraphAPIGenericException(err_msg)


class GraphAPIGenericException(RuntimeError):
    pass
