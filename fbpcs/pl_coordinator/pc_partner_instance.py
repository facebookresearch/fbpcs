#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import Any, Dict, List, Optional

from fbpcs.pl_coordinator.constants import WAIT_VALID_STATUS_TIMEOUT
from fbpcs.pl_coordinator.pc_calc_instance import PrivateLiftCalcInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
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
    get_instance,
    run_stage,
)


# TODO(T107103724): [BE] rename PrivateLiftPartnerInstance
class PrivateLiftPartnerInstance(PrivateLiftCalcInstance):
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
        try:
            self.status: PrivateComputationInstanceStatus = get_instance(
                self.config, self.instance_id, self.logger
            ).status
        except RuntimeError:
            self.logger.info(f"Creating new partner instance {self.instance_id}")
            self.status = create_instance(
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
            ).status
        self.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)

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
