#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.dummy_stage_service import (
    DummyStageService,
)
from fbpcs.private_computation.service.id_spine_combiner_stage_service import (
    IdSpineCombinerStageService,
)
from fbpcs.private_computation.service.pcf2_aggregation_stage_service import (
    PCF2AggregationStageService,
)
from fbpcs.private_computation.service.pcf2_attribution_stage_service import (
    PCF2AttributionStageService,
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


class PrivateComputationPCF2LocalTestStageFlow(PrivateComputationBaseStageFlow):
    """
    This enum lists all of the supported stage types and maps to their possible statuses.
    It also provides methods to get information about the next or previous stage.

    NOTE:
    1. This is enum contains the flow - ID MATCH -> ID_SPINE_COMBINER -> RESHARD -> PCF2_ATTRIBUTION -> PCF2_AGGREGATION -> SHARD AGGREGATION.
    2. The order in which the enum members appear is the order in which the stages are intended
    to run. The _order_ variable is used to ensure member order is consistent (class attribute, removed during class creation).
    An exception is raised at runtime if _order_ is inconsistent with the actual member order.
    3. This flow currently should only be used for PA.
    """

    # Specifies the order of the stages. Don't change this unless you know what you are doing.
    # pyre-fixme[15]: `_order_` overrides attribute defined in `Enum` inconsistently.
    _order_ = (
        "CREATED ID_SPINE_COMBINER RESHARD PCF2_ATTRIBUTION PCF2_AGGREGATION AGGREGATE"
    )
    # Regarding typing fixme above, Pyre appears to be wrong on this one. _order_ only appears in the EnumMeta metaclass __new__ method
    # and is not actually added as a variable on the enum class. I think this is why pyre gets confused.

    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
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
    PCF2_ATTRIBUTION = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_STARTED,
        PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_COMPLETED,
        PrivateComputationInstanceStatus.PCF2_ATTRIBUTION_FAILED,
        True,
    )
    PCF2_AGGREGATION = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PCF2_AGGREGATION_STARTED,
        PrivateComputationInstanceStatus.PCF2_AGGREGATION_COMPLETED,
        PrivateComputationInstanceStatus.PCF2_AGGREGATION_FAILED,
        True,
    )
    AGGREGATE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
        PrivateComputationInstanceStatus.AGGREGATION_FAILED,
        True,
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
        elif self is self.PCF2_ATTRIBUTION:
            return PCF2AttributionStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.PCF2_AGGREGATION:
            return PCF2AggregationStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif self is self.AGGREGATE:
            return AggregateShardsStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        else:
            raise NotImplementedError(f"No stage service configured for {self}")
