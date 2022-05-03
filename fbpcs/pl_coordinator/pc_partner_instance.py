#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import Any, Dict, List, Optional

from fbpcs.pl_coordinator.constants import WAIT_VALID_STATUS_TIMEOUT
from fbpcs.pl_coordinator.exceptions import PCInstanceCalculationException
from fbpcs.pl_coordinator.pc_calc_instance import PrivateComputationCalcInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    AggregationType,
    AttributionRule,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    cancel_current_stage,
    create_instance,
    update_input_path,
    get_instance,
    run_stage,
)


class PrivateComputationPartnerInstance(PrivateComputationCalcInstance):
    """
    Representation of a partner instance.
    """

    def __init__(
        self,
        instance_id: str,
        config: Dict[str, Any],
        input_path: str,
        num_mpc_containers: int,
        num_pid_containers: int,
        logger: logging.Logger,
        game_type: PrivateComputationGameType,
        attribution_rule: Optional[AttributionRule] = None,
        aggregation_type: Optional[AggregationType] = None,
        concurrency: Optional[int] = None,
        num_files_per_mpc_container: Optional[int] = None,
        k_anonymity_threshold: Optional[int] = None,
    ) -> None:
        super().__init__(instance_id, logger, PrivateComputationRole.PARTNER)
        self.config: Dict[str, Any] = config
        self.input_path: str = input_path
        self.output_dir: str = self.get_output_dir_from_input_path(input_path)
        # try to get instance from instance repo, if not, create a new instance
        self.status: PrivateComputationInstanceStatus
        pc_instance: PrivateComputationInstance
        try:
            pc_instance = get_instance(self.config, self.instance_id, self.logger)
        except RuntimeError:
            self.logger.info(f"Creating new partner instance {self.instance_id}")
            pc_instance = create_instance(
                config=self.config,
                instance_id=self.instance_id,
                role=PrivateComputationRole.PARTNER,
                game_type=game_type,
                logger=self.logger,
                input_path=self.input_path,
                output_dir=self.output_dir,
                num_pid_containers=num_pid_containers,
                num_mpc_containers=num_mpc_containers,
                attribution_rule=attribution_rule,
                aggregation_type=aggregation_type,
                concurrency=concurrency,
                num_files_per_mpc_container=num_files_per_mpc_container,
                k_anonymity_threshold=k_anonymity_threshold,
            )

        self.status = pc_instance.status
        if self._need_override_input_path(pc_instance):
            update_input_path(
                self.config, self.instance_id, self.input_path, self.logger
            )

        self.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)

    def _need_override_input_path(self, instance: PrivateComputationInstance) -> bool:
        """
        we check partner stage status to see if it's able to override input
        """
        if self.input_path == instance.input_path:
            return False

        if self.status in (
            PrivateComputationInstanceStatus.CREATED,
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_FAILED,
        ):
            return True
        else:
            raise PCInstanceCalculationException(
                f"Unable to override input path {self.input_path} to exisiting instance input path {instance.input_path}",
                f"input path can't be updated as the current status is too late to override {self.status}",
                "Please wait 24 hours for the instance to expire or contact your representative at meta for assistance",
            )

    def update_instance(self) -> None:
        self.status = get_instance(self.config, self.instance_id, self.logger).status

    def cancel_current_stage(self) -> None:
        cancel_current_stage(self.config, self.instance_id, self.logger)

    def get_output_dir_from_input_path(self, input_path: str) -> str:
        return input_path[: input_path.rfind("/")]

    def run_stage(
        self,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        if self.should_invoke_operation(stage):
            try:
                run_stage(
                    config=self.config,
                    instance_id=self.instance_id,
                    stage=stage,
                    logger=self.logger,
                    server_ips=server_ips,
                )
            except Exception as error:
                self.logger.exception(f"Error running partner {stage.name} {error}")
