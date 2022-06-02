#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

from fbpcs.private_computation.service.compute_metrics_stage_service import (
    ComputeMetricsStageService,
)

from fbpcs.private_computation.service.pid_run_protocol_stage_service import (
    PIDRunProtocolStageService,
)

from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)


class PrivateComputationPIDMigrationTestStageFlow(PrivateComputationBaseStageFlow):
    """
    The purpose of this stage flow is to make sure the migration of PIDService to PrivateComputationStageFlow
    work as expected. So only PID_SHARD, PID_PREPARE, ID_MATCH stages will use new stage services that to be tested,
    and all other stages are kept the same. If the E2E test of this test stage flow work as the StageFlow,
    then PIDService migration succeeds.

    NOTE: The order in which the enum members appear is the order in which the stages are intended
    to run. The _order_ variable is used to ensure member order is consistent (class attribute, removed during class creation).
    An exception is raised at runtime if _order_ is inconsistent with the actual member order.
    """

    # Specifies the order of the stages. Don't change this unless you know what you are doing.
    # pyre-fixme[15]: `_order_` overrides attribute defined in `Enum` inconsistently.
    _order_ = "CREATED INPUT_DATA_VALIDATION PID_SHARD PID_PREPARE ID_MATCH ID_MATCH_POST_PROCESS ID_SPINE_COMBINER RESHARD COMPUTE AGGREGATE POST_PROCESSING_HANDLERS"
    # Regarding typing fixme above, Pyre appears to be wrong on this one. _order_ only appears in the EnumMeta metaclass __new__ method
    # and is not actually added as a variable on the enum class. I think this is why pyre gets confused.

    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )
    INPUT_DATA_VALIDATION = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_STARTED,
        PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED,
        PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_FAILED,
        False,
    )
    PID_SHARD = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PID_SHARD_STARTED,
        PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
        PrivateComputationInstanceStatus.PID_SHARD_FAILED,
        False,
    )
    PID_PREPARE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PID_PREPARE_STARTED,
        PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED,
        PrivateComputationInstanceStatus.PID_PREPARE_FAILED,
        False,
    )
    ID_MATCH = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        True,
        is_retryable=False,
    )
    ID_MATCH_POST_PROCESS = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_FAILED,
        False,
    )
    ID_SPINE_COMBINER = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
        PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
        PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
        False,
    )
    RESHARD = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.RESHARD_STARTED,
        PrivateComputationInstanceStatus.RESHARD_COMPLETED,
        PrivateComputationInstanceStatus.RESHARD_FAILED,
        False,
    )
    COMPUTE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.COMPUTATION_STARTED,
        PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
        PrivateComputationInstanceStatus.COMPUTATION_FAILED,
        True,
    )
    AGGREGATE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
        PrivateComputationInstanceStatus.AGGREGATION_FAILED,
        True,
    )
    POST_PROCESSING_HANDLERS = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED,
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED,
        False,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        """
        Maps PrivateComputationStageFlow instances to StageService instances

        Arguments:
            args: Common arguments initialized in PrivateComputationService that are consumed by stage services

        Returns:
            An instantiated StageService object corresponding to the StageFlow enum member caller.

        Raises:
            NotImplementedError: The subclass doesn't implement a stage service for a given StageFlow enum member
        """
        if self is self.ID_MATCH:
            return PIDRunProtocolStageService(
                args.storage_svc,
                args.onedocker_svc,
                args.onedocker_binary_config_map,
                args.pid_svc.multikey_enabled,
            )
        elif self is self.COMPUTE:
            return ComputeMetricsStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        else:
            return self.get_default_stage_service(args)
