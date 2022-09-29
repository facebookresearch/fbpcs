#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.decoupled_aggregation_stage_service import (
    AggregationStageService,
)
from fbpcs.private_computation.service.decoupled_attribution_stage_service import (
    AttributionStageService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)


class PrivateComputationDecoupledLocalTestStageFlow(PrivateComputationBaseStageFlow):
    """
    This enum lists all of the supported stage types and maps to their possible statuses.
    It also provides methods to get information about the next or previous stage.

    NOTE:
    1. This is enum contains the flow - ID MATCH -> ID_SPINE_COMBINER -> RESHARD -> ATTRIBUTION -> AGGREGATION -> SHARD AGGREGATION.
    2. The order in which the enum members appear is the order in which the stages are intended
    to run. The _order_ variable is used to ensure member order is consistent (class attribute, removed during class creation).
    An exception is raised at runtime if _order_ is inconsistent with the actual member order.
    3. This flow currently should only be used for PA.
    """

    # Specifies the order of the stages. Don't change this unless you know what you are doing.
    # pyre-fixme[15]: `_order_` overrides attribute defined in `Enum` inconsistently.
    _order_ = "CREATED PID_SHARD PID_PREPARE ID_MATCH ID_MATCH_POST_PROCESS ID_SPINE_COMBINER RESHARD DECOUPLED_ATTRIBUTION DECOUPLED_AGGREGATION AGGREGATE"
    # Regarding typing fixme above, Pyre appears to be wrong on this one. _order_ only appears in the EnumMeta metaclass __new__ method
    # and is not actually added as a variable on the enum class. I think this is why pyre gets confused.

    CREATED = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.CREATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.CREATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.CREATED,
        failed_status=PrivateComputationInstanceStatus.CREATION_FAILED,
        is_joint_stage=False,
    )
    PID_SHARD = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.PID_SHARD_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.PID_SHARD_STARTED,
        completed_status=PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.PID_SHARD_FAILED,
        is_joint_stage=False,
    )
    PID_PREPARE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.PID_PREPARE_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.PID_PREPARE_STARTED,
        completed_status=PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.PID_PREPARE_FAILED,
        is_joint_stage=False,
    )
    ID_MATCH = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.ID_MATCHING_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        completed_status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        is_joint_stage=True,
        is_retryable=True,
    )
    ID_MATCH_POST_PROCESS = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_STARTED,
        completed_status=PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.ID_MATCHING_POST_PROCESS_FAILED,
        is_joint_stage=False,
    )
    ID_SPINE_COMBINER = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.ID_SPINE_COMBINER_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.ID_SPINE_COMBINER_STARTED,
        completed_status=PrivateComputationInstanceStatus.ID_SPINE_COMBINER_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED,
        is_joint_stage=False,
    )
    RESHARD = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.RESHARD_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.RESHARD_STARTED,
        completed_status=PrivateComputationInstanceStatus.RESHARD_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.RESHARD_FAILED,
        is_joint_stage=False,
    )
    DECOUPLED_ATTRIBUTION = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_STARTED,
        completed_status=PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_FAILED,
        is_joint_stage=True,
    )
    DECOUPLED_AGGREGATION = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_FAILED,
        is_joint_stage=True,
    )
    AGGREGATE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.AGGREGATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.AGGREGATION_FAILED,
        is_joint_stage=True,
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
        if self is self.DECOUPLED_ATTRIBUTION:
            return AttributionStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.DECOUPLED_AGGREGATION:
            return AggregationStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        else:
            return self.get_default_stage_service(args)
