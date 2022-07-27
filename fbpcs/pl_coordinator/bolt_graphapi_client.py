#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import dateutil

import requests

from fbpcs.bolt.bolt_client import BoltClient, BoltState
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN
from fbpcs.pl_coordinator.exceptions import GraphAPITokenNotFound, IncorrectVersionError
from fbpcs.private_computation.entity.pcs_tier import PCSTier
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import AttributionRule
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation_cli.private_computation_service_wrapper import get_tier
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
TERMINAL_STATUSES = [
    "POST_PROCESSING_HANDLERS_COMPLETED",
    "RESULT_READY",
    "INSTANCE_FAILURE",
]


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
        self.config = config
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )
        self.access_token = self._get_graph_api_token(config["graphapi"])
        self.params = {"access_token": self.access_token}

    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        if isinstance(instance_args, BoltPLGraphAPICreateInstanceArgs):
            response = await self._create_instance(instance_args)
            return response.json().id
        elif isinstance(instance_args, BoltPAGraphAPICreateInstanceArgs):
            instance_id = await self._get_existing_pa_instance_id(instance_args)
            if instance_id:
                instance_args.instance_id = instance_id
            else:
                response = await self._create_instance(instance_args)
                instance_id = response.json().id
                instance_args.instance_id = instance_id

            instance_data = await self._get_pa_instance_info(instance_id)
            self._check_version(instance_data, self.config)
            return instance_id
        else:
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
        params["operation"] = "NEXT"
        r = requests.post(f"{URL}/{instance_id}", params=params)
        if stage:
            msg = f"running stage {stage}"
        else:
            msg = "running next stage"
        self._check_err(r, msg)

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
        if not expected_result_path:
            self.logger.info(
                "No expected result path was given, so result validation was skipped."
            )
            return True
        else:
            raise NotImplementedError(
                "This method should not be called with expected results"
            )

    async def is_existing_instance(self, instance_args: BoltCreateInstanceArgs) -> bool:
        instance_id = instance_args.instance_id
        self.logger.info(f"Checking if {instance_id} exists...")
        if instance_id:
            try:
                await self.update_instance(instance_id)
                self.logger.info(f"{instance_id} found.")
                return True
            except Exception:
                self.logger.info(f"{instance_id} not found.")
                return False
        else:
            self.logger.info("instance_id is empty, fetching a valid one")
            return False

    async def get_instance(self, instance_id: str) -> requests.Response:
        r = requests.get(f"{URL}/{instance_id}", self.params)
        self._check_err(r, "getting fb instance")
        return r

    async def _create_instance(
        self, instance_args: BoltCreateInstanceArgs
    ) -> requests.Response:
        if isinstance(instance_args, BoltPLGraphAPICreateInstanceArgs):
            self.logger.info("Creating new PL instance...")
            params = self.params.copy()
            params["breakdown_key"] = json.dumps(instance_args.breakdown_key)
            r = requests.post(
                f"{URL}/{instance_args.study_id}/instances", params=params
            )
            self._check_err(r, "creating fb pl instance")
            return r
        elif isinstance(instance_args, BoltPAGraphAPICreateInstanceArgs):
            self.logger.info("Creating new PA instance...")
            params = self.params.copy()
            params["attribution_rule"] = instance_args.attribution_rule
            params["timestamp"] = instance_args.timestamp
            r = requests.post(
                f"{URL}/{instance_args.dataset_id}/instance", params=params
            )
            self._check_err(r, "creating fb pa instance")
            return r
        else:
            raise TypeError(
                f"Instance args must be of type {BoltPLGraphAPICreateInstanceArgs} or {BoltPAGraphAPICreateInstanceArgs}"
            )

    async def _get_existing_pa_instance_id(
        self, instance_args: BoltPAGraphAPICreateInstanceArgs
    ) -> Optional[str]:
        self.logger.info("Getting valid PA instance ids...")
        params = self.params.copy()
        r = requests.get(f"{URL}/{instance_args.dataset_id}/instances", params=params)
        self._check_err(r, "getting attribution instances tied to the dataset")
        dataset_instance_data = json.loads(r.text)
        existing_instances = dataset_instance_data["data"]
        for inst in existing_instances:
            inst_time = dateutil.parser.parse(inst["timestamp"])
            creation_time = dateutil.parser.parse(inst["created_time"])
            exp_time = datetime.now(tz=timezone.utc) - timedelta(days=1)
            expired = exp_time > creation_time
            if (
                inst["attribution_rule"]
                == AttributionRule[instance_args.attribution_rule].value
                and inst_time == datetime.fromtimestamp(int(instance_args.timestamp))
                and inst["status"] not in TERMINAL_STATUSES
                and not expired
            ):
                instance_id = inst["id"]
                self.logger.info(f"Valid PA instance id found: {instance_id}")
                return instance_id
        self.logger.info("No valid PA instance id found.")
        return None

    async def _get_pa_instance_info(self, instance_id: str) -> Any:
        response = await self.get_instance(instance_id)
        return json.loads(response.text)

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

    def _check_version(
        self,
        instance: Dict[str, Any],
        config: Dict[str, Any],
    ) -> None:
        """Checks that the publisher version (graph api) and the partner version (config.yml) are the same

        Arguments:
            instances: theoretically is dict representing the PA instance fields.
            config: The dict representation of a config.yml file

        Raises:
            IncorrectVersionError: the publisher and partner are running with different versions
        """

        instance_tier_str = instance.get("tier")
        # if there is no tier for some reason, let's just assume
        # the tier is correct
        if not instance_tier_str:
            return

        config_tier = get_tier(config)
        expected_tier = PCSTier.from_str(instance_tier_str)
        if expected_tier is not config_tier:
            raise IncorrectVersionError.make_error(
                instance["id"], expected_tier, config_tier
            )

    def _check_err(self, r: requests.Response, msg: str) -> None:
        if r.status_code != 200:
            err_msg = f"Error {msg}: {r.content}"
            self.logger.error(err_msg)
            raise RuntimeError(err_msg)
