#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from enum import Enum


class PrivateComputationStageType(Enum):
    """
    This enum lists all of the supported stage types and maps to their possible statuses.
    It also provides methods to get information about the next or previous stage.

    NOTE: The order in which the enum members appear is the order in which the stages are intended
    to be ran. The _order_ variable is used to ensure member order is consistent (class attribute, removed during class creation).
    An exception is raised at runtime if _order_ is inconsistent with the actual member order.
    """


    # Specifies the order of the stages. Don't change this unless you know what you are doing.
    # pyre-fixme[15]: `_order_` overrides attribute defined in `Enum` inconsistently.
    _order_ = "UNKNOWN CREATED ID_MATCH COMPUTE AGGREGATE POST_PROCESSING_HANDLERS"
    # Regarding typing fixme above, Pyre appears to be wrong on this one. _order_ only appears in the EnumMeta metaclass __new__ method
    # and is not actually added as a variable on the enum class. I think this is why pyre gets confused.

    # map member name -> start, complete, and fail status
    UNKNOWN = (
        PrivateComputationInstanceStatus.UNKNOWN,
        PrivateComputationInstanceStatus.UNKNOWN,
        PrivateComputationInstanceStatus.UNKNOWN,
    )
    CREATED = (
        PrivateComputationInstanceStatus.UNKNOWN,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.UNKNOWN,
    )
    ID_MATCH = (
        PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
    )
    COMPUTE = (
        PrivateComputationInstanceStatus.COMPUTATION_STARTED,
        PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
        PrivateComputationInstanceStatus.COMPUTATION_FAILED,
    )
    AGGREGATE = (
        PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
        PrivateComputationInstanceStatus.AGGREGATION_FAILED,
    )
    POST_PROCESSING_HANDLERS = (
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED,
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
        PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED,
    )

    def __init__(
        self,
        start_status: PrivateComputationInstanceStatus,
        completed_status: PrivateComputationInstanceStatus,
        failed_status: PrivateComputationInstanceStatus,
    ) -> None:
        self.start_status = start_status
        self.completed_status = completed_status
        self.failed_status = failed_status

    @property
    def next_stage(self) -> "PrivateComputationStageType":
        """
        Get the enum member representing the next stage in the sequence
        """
        members = list(self.__class__)
        index = (members.index(self) + 1) % len(members)
        return members[index]

    @property
    def previous_stage(self) -> "PrivateComputationStageType":
        """
        Get the enum member representing the previous stage in the sequence
        """
        members = list(self.__class__)
        index = max(0, members.index(self) - 1)
        return members[index]
