#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from fbpcs.bolt.bolt_client import BoltClient, BoltState
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict
from fbpcs.utils.config_yaml.exceptions import ConfigYamlBaseException

URL = "https://graph.facebook.com/v13.0"
GRAPHAPI_INSTANCE_STATUSES: Dict[str, PrivateComputationInstanceStatus] = {
    "CREATED": PrivateComputationInstanceStatus.CREATED,
    # INPUT_DATA_VALIDATION_XXX statuses mapping to PC_PRE_VALIDATION_XXX
    # for backwards compatibility see context here: https://fburl.com/dkol8bma
    "INPUT_DATA_VALIDATION_STARTED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
    "INPUT_DATA_VALIDATION_COMPLETED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
    "INPUT_DATA_VALIDATION_FAILED": PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
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


@dataclass
class BoltPLGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    study_id: str
    breakdown_key: Dict[str, str]


@dataclass
class BoltPAGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    dataset_id: str
    timestamp: str
    attribution_rule: str
    num_containers: str


class BoltGraphAPIClient(BoltClient):
    def __init__(
        self, config: Dict[str, Any], logger: Optional[logging.Logger] = None
    ) -> None:
        """Bolt GraphAPI Client

        Args:
            - config: the graphapi section of the larger config dictionary: config["graphapi"]
            - logger: logger
        """
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )
        self.access_token = self._get_graph_api_token(config)
        self.params = {"access_token": self.access_token}

    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        params = self.params.copy()
        if isinstance(instance_args, BoltPLGraphAPICreateInstanceArgs):
            params["breakdown_key"] = json.dumps(instance_args.breakdown_key)
            r = requests.post(
                f"{URL}/{instance_args.study_id}/instances", params=params
            )
            self._check_err(r, "creating fb pl instance")
            return r.json().id
        elif isinstance(instance_args, BoltPAGraphAPICreateInstanceArgs):
            params["attribution_rule"] = instance_args.attribution_rule
            params["timestamp"] = instance_args.timestamp
            r = requests.post(
                f"{URL}/{instance_args.dataset_id}/instance", params=params
            )
            self._check_err(r, "creating fb pa instance")
            return r.json().id
        raise TypeError(
            f"Instance args must be of type {BoltPLGraphAPICreateInstanceArgs} or {BoltPAGraphAPICreateInstanceArgs}"
        )

    async def run_stage(
        self,
        instance_id: str,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        params = self.params.copy()
        if stage:
            if await self.should_invoke_stage(instance_id=instance_id, stage=stage):
                params["operation"] = "NEXT"
                r = requests.post(f"{URL}/{instance_id}", params=params)
                self._check_err(r, f"running stage {stage}")
            else:
                raise RuntimeError(f"{instance_id} should not invoke stage {stage}")
        else:
            params["operation"] = "NEXT"
            r = requests.post(f"{URL}/{instance_id}", params=params)
            self._check_err(r, f"running stage {stage}")

    async def update_instance(self, instance_id: str) -> BoltState:
        response = json.loads((await self.get_instance(instance_id)).text)
        response_status = response.get("status")
        try:
            status = GRAPHAPI_INSTANCE_STATUSES[response_status]
        except KeyError:
            raise RuntimeError(
                f"Error getting status: Unexpected value {response_status}"
            )
        server_ips = response.get("server_ips")
        return BoltState(status, server_ips)

    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        pass

    async def get_instance(self, instance_id: str) -> requests.Response:
        r = requests.get(f"{URL}/{instance_id}", self.params)
        self._check_err(r, "getting fb instance")
        return r

    async def should_invoke_stage(
        self, instance_id: str, stage: PrivateComputationBaseStageFlow
    ) -> bool:
        status = (
            await self.update_instance(instance_id=instance_id)
        ).pc_instance_status
        # should only run this stage if previous one is finished or
        # stage is failed (retrying)
        return (
            self.ready_for_stage(status, stage) and status is not stage.started_status
        )

    def _get_graph_api_token(self, config: Dict[str, Any]) -> str:
        """Get graph API token from config.yml or the {FBPCS_GRAPH_API_TOKEN} env var

        Args:
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
            token = config["access_token"]
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

    def _check_err(self, r: requests.Response, msg: str) -> None:
        if r.status_code != 200:
            err_msg = f"Error {msg}: {r.content}"
            self.logger.error(err_msg)
            raise RuntimeError(err_msg)
