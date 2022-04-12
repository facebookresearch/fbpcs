#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.pid.entity.pid_instance import UnionPIDStage
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.decoupled_aggregation_stage_service import (
    AggregationStageService,
)
from fbpcs.private_computation.service.decoupled_attribution_stage_service import (
    AttributionStageService,
)
from fbpcs.private_computation.service.dummy_stage_service import (
    DummyStageService,
)
from fbpcs.private_computation.service.id_spine_combiner_stage_service import (
    IdSpineCombinerStageService,
)
from fbpcs.private_computation.service.input_data_validation_stage_service import (
    InputDataValidationStageService,
)
from fbpcs.private_computation.service.pid_stage_service import PIDStageService
from fbpcs.private_computation.service.post_processing_stage_service import (
    PostProcessingStageService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.service.shard_stage_service import ShardStageService
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)


class PrivateComputationDecoupledStageFlow(PrivateComputationBaseStageFlow):
    """
    - Private Attribution Stage Flow -
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
    _order_ = "CREATED INPUT_DATA_VALIDATION PID_SHARD PID_PREPARE ID_MATCH ID_MATCH_POST_PROCESS ID_SPINE_COMBINER RESHARD DECOUPLED_ATTRIBUTION DECOUPLED_AGGREGATION AGGREGATE POST_PROCESSING_HANDLERS"
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
    DECOUPLED_ATTRIBUTION = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_STARTED,
        PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_COMPLETED,
        PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_FAILED,
        True,
    )
    DECOUPLED_AGGREGATION = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_STARTED,
        PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_COMPLETED,
        PrivateComputationInstanceStatus.DECOUPLED_AGGREGATION_FAILED,
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
        if self is self.CREATED:
            return DummyStageService()
        elif self is self.INPUT_DATA_VALIDATION:
            return InputDataValidationStageService(
                args.pc_validator_config,
                args.onedocker_svc,
                args.onedocker_binary_config_map,
            )
        elif self is self.PID_SHARD:
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_SHARD,
                UnionPIDStage.ADV_SHARD,
            )
        elif self is self.PID_PREPARE:
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_PREPARE,
                UnionPIDStage.ADV_PREPARE,
            )
        elif self is self.ID_MATCH:
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_RUN_PID,
                UnionPIDStage.ADV_RUN_PID,
            )
        elif self is self.ID_MATCH_POST_PROCESS:
            return PostProcessingStageService(
                args.storage_svc, args.pid_post_processing_handlers
            )
        elif self is self.ID_SPINE_COMBINER:
            return IdSpineCombinerStageService(
                args.onedocker_svc,
                args.onedocker_binary_config_map,
            )
        elif self is self.RESHARD:
            return ShardStageService(
                args.onedocker_svc,
                args.onedocker_binary_config_map,
            )
        elif self is self.DECOUPLED_ATTRIBUTION:
            return AttributionStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.DECOUPLED_AGGREGATION:
            return AggregationStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.AGGREGATE:
            return AggregateShardsStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.POST_PROCESSING_HANDLERS:
            return PostProcessingStageService(
                args.storage_svc, args.post_processing_handlers
            )
        else:
            raise NotImplementedError(f"No stage service configured for {self}")
