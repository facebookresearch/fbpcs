#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from fbpcs.bolt.bolt_client import BoltClient, BoltState
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.constants import FBPCS_GRAPH_API_TOKEN
from fbpcs.pl_coordinator.exceptions import (
    GraphAPIGenericException,
    GraphAPITokenNotFound,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict
from fbpcs.utils.config_yaml.exceptions import ConfigYamlBaseException

GRAPHAPI_HTTPS = "https://"
GRAPHAPI_DEFAULT_DOMAIN = "graph.facebook.com"
GRAPHAPI_DEFAULT_VERSION = "v13.0"

GRAPHAPI_INSTANCE_STATUSES: Dict[str, PrivateComputationInstanceStatus] = {
    **{status.value: status for status in PrivateComputationInstanceStatus},
    **{
        "INSTANCE_FAILURE": PrivateComputationInstanceStatus.UNKNOWN,
        "ID_MATCH_STARTED": PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        "ID_MATCH_COMPLETED": PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        "ID_MATCH_FAILED": PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        "RESULT_READY": PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
    },
}


@dataclass
class BoltPLGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    study_id: str
    breakdown_key: Dict[str, str]
    run_id: Optional[str]


@dataclass
class BoltPAGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    dataset_id: str
    timestamp: str
    attribution_rule: str


BoltGraphAPICreateInstanceArgs = TypeVar(
    "BoltGraphAPICreateInstanceArgs",
    BoltPLGraphAPICreateInstanceArgs,
    BoltPAGraphAPICreateInstanceArgs,
)


class BoltGraphAPIClient(BoltClient[BoltGraphAPICreateInstanceArgs]):
    def __init__(
        self,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None,
        graphapi_version: Optional[str] = None,
        graphapi_domain: Optional[str] = None,
    ) -> None:
        """Bolt GraphAPI Client

        Args:
            - config: the graphapi section of the larger config dictionary: config["graphapi"]
            - logger: logger
            - graphapi_version: version to use, e.g. "v13.0" or "v14.0"
            - graphapi_domain: domain, e.g. "graph.facebook.com"
        """

        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )
        _graphapi_version = graphapi_version or GRAPHAPI_DEFAULT_VERSION
        _graphapi_domain = graphapi_domain or GRAPHAPI_DEFAULT_DOMAIN
        self.graphapi_url = f"{GRAPHAPI_HTTPS}{_graphapi_domain}/{_graphapi_version}"
        self.logger.info(f"GraphAPI URL: {self.graphapi_url}")
        self.access_token = self._get_graph_api_token(config)
        self.params = {"access_token": self.access_token}

    async def create_instance(
        self,
        instance_args: BoltGraphAPICreateInstanceArgs,
    ) -> str:
        params = self.params.copy()
        if isinstance(instance_args, BoltPLGraphAPICreateInstanceArgs):
            params["breakdown_key"] = json.dumps(instance_args.breakdown_key)
            if instance_args.run_id is not None:
                params["run_id"] = instance_args.run_id
            r = requests.post(
                f"{self.graphapi_url}/{instance_args.study_id}/instances", params=params
            )
            self._check_err(r, "creating fb pl instance")
            return r.json()["id"]
        elif isinstance(instance_args, BoltPAGraphAPICreateInstanceArgs):
            params["attribution_rule"] = instance_args.attribution_rule
            params["timestamp"] = instance_args.timestamp
            r = requests.post(
                f"{self.graphapi_url}/{instance_args.dataset_id}/instance",
                params=params,
            )
            self._check_err(r, "creating fb pa instance")
            return r.json()["id"]
        raise TypeError(
            f"Instance args must be of type {BoltPLGraphAPICreateInstanceArgs} or {BoltPAGraphAPICreateInstanceArgs}"
        )

    async def get_stage_flow(
        self, instance_id: str
    ) -> Optional[Type[PrivateComputationBaseStageFlow]]:
        """GraphAPI didn't return stageflow info"""
        return None

    async def run_stage(
        self,
        instance_id: str,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        params = self.params.copy()
        params["operation"] = "NEXT"
        r = requests.post(f"{self.graphapi_url}/{instance_id}", params=params)
        if stage:
            msg = f"running stage {stage}"
        else:
            msg = "running next stage"
        self._check_err(r, msg)

    async def cancel_current_stage(
        self,
        instance_id: str,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        params = self.params.copy()
        params["operation"] = "CANCEL"
        r = requests.post(f"{self.graphapi_url}/{instance_id}", params=params)
        if stage:
            msg = f"cancel current stage {stage}."
        else:
            msg = "cancel current stage."
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

    async def is_existing_instance(
        self,
        instance_args: BoltGraphAPICreateInstanceArgs,
    ) -> bool:
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

    async def has_feature(self, instance_id: str, feature: PCSFeature) -> bool:
        response = json.loads((await self.get_instance(instance_id)).text)
        feature_list = response.get("feature_list")
        if feature_list:
            pcs_feature_enums = {
                PCSFeature.from_str(feature) for feature in feature_list
            }
            return feature in pcs_feature_enums
        return False

    async def get_instance(self, instance_id: str) -> requests.Response:
        r = requests.get(f"{self.graphapi_url}/{instance_id}", self.params)
        self._check_err(r, "getting fb instance")
        return r

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
            config = config.get("graphapi", config)
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
                raise no_token_exception from None
            self.logger.info(
                f"successfully read graph api token from {FBPCS_GRAPH_API_TOKEN} env var"
            )
        return token

    def _check_err(self, r: requests.Response, msg: str) -> None:
        if r.status_code != 200:
            err_msg = f"Error {msg}: {r.content}"
            self.logger.error(err_msg)
            raise GraphAPIGenericException(err_msg)

    def get_adspixels(self, adspixels_id: str, fields: List[str]) -> requests.Response:
        params = self.params.copy()
        params["fields"] = ",".join(fields)
        r = requests.get(f"{self.graphapi_url}/{adspixels_id}", params=params)
        self._check_err(r, "getting adspixels data")
        return r

    def get_debug_token_data(self) -> requests.Response:
        params = self.params.copy()
        params["input_token"] = self.access_token
        r = requests.get(f"{self.graphapi_url}/debug_token", params=params)
        self._check_err(r, "getting debug token data")
        return r

    def get_study_data(self, study_id: str, fields: List[str]) -> requests.Response:
        params = self.params.copy()
        params["fields"] = ",".join(fields)
        r = requests.get(f"{self.graphapi_url}/{study_id}", params=params)
        self._check_err(r, "getting study data")
        return r

    def get_attribution_dataset_info(
        self, dataset_id: str, fields: List[str]
    ) -> requests.Response:
        params = self.params.copy()
        params["fields"] = ",".join(fields)
        r = requests.get(f"{self.graphapi_url}/{dataset_id}", params=params)
        self._check_err(r, "getting dataset information")
        return r

    def get_existing_pa_instances(self, dataset_id: str) -> requests.Response:
        params = self.params.copy()
        r = requests.get(f"{self.graphapi_url}/{dataset_id}/instances", params=params)
        self._check_err(r, "getting attribution instances tied to the dataset")
        return r
