#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)
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
from fbpcs.private_computation.service.id_match_stage_service import IdMatchStageService
from fbpcs.private_computation.service.post_processing_stage_service import (
    PostProcessingStageService,
)
from fbpcs.private_computation.service.prepare_data_stage_service import (
    PrepareDataStageService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)


class PrivateComputationDecoupledStageFlow(PrivateComputationBaseStageFlow):
    """
    This enum lists all of the supported stage types and maps to their possible statuses.
    It also provides methods to get information about the next or previous stage.

    NOTE:
    1. This is enum contains the flow - ID MATCH -> PREPARE -> ATTRIBUTION -> AGGREGATION -> SHARD AGGREGATION.
    2. The order in which the enum members appear is the order in which the stages are intended
    to be ran. The _order_ variable is used to ensure member order is consistent (class attribute, removed during class creation).
    An exception is raised at runtime if _order_ is inconsistent with the actual member order.
    3. This flow currently should only be used for PA.
    """

    # Specifies the order of the stages. Don't change this unless you know what you are doing.
    # pyre-fixme[15]: `_order_` overrides attribute defined in `Enum` inconsistently.
    _order_ = (
        "CREATED ID_MATCH PREPARE DECOUPLED_ATTRIBUTION DECOUPLED_AGGREGATION AGGREGATE POST_PROCESSING_HANDLERS"
    )
    # Regarding typing fixme above, Pyre appears to be wrong on this one. _order_ only appears in the EnumMeta metaclass __new__ method
    # and is not actually added as a variable on the enum class. I think this is why pyre gets confused.

    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )
    ID_MATCH = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        True,
    )
    PREPARE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PREPARE_DATA_STARTED,
        PrivateComputationInstanceStatus.PREPARE_DATA_COMPLETED,
        PrivateComputationInstanceStatus.PREPARE_DATA_FAILED,
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
        elif self is self.ID_MATCH:
            return IdMatchStageService(
                args.pid_svc,
                args.pid_config,
            )
        elif self is self.PREPARE:
            return PrepareDataStageService(
                args.onedocker_svc,
                args.onedocker_binary_config_map,
                update_status_to_complete=True,
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
