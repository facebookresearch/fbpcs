#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import abstractmethod
from dataclasses import dataclass
from typing import Type, TypeVar, TYPE_CHECKING

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

if TYPE_CHECKING:
    from fbpcs.private_computation.service.private_computation_stage_service import (
        PrivateComputationStageService,
        PrivateComputationStageServiceArgs,
    )
from fbpcs.stage_flow.stage_flow import StageFlow, StageFlowData

C = TypeVar("C", bound="PrivateComputationBaseStageFlow")


@dataclass(frozen=True)
class PrivateComputationStageFlowData(StageFlowData[PrivateComputationInstanceStatus]):
    is_joint_stage: bool


class PrivateComputationBaseStageFlow(StageFlow):
    # TODO(T103297566): [BE] document PrivateComputationBaseStageFlow
    def __init__(self, data: PrivateComputationStageFlowData) -> None:
        super().__init__()
        self.started_status: PrivateComputationInstanceStatus = data.started_status
        self.failed_status: PrivateComputationInstanceStatus = data.failed_status
        self.completed_status: PrivateComputationInstanceStatus = data.completed_status
        self.is_joint_stage: bool = data.is_joint_stage

    @classmethod
    def cls_name_to_cls(cls: Type[C], name: str) -> Type[C]:
        """
        Converts the name of an existing stage flow subclass into the subclass object

        Arguments:
            name: The name of a PrivateComputationBaseStageFlow subclass

        Returns:
            A subclass of PrivateComputationBaseStageFlow

        Raises:
            ValueError: raises when no subclass with the name 'name' is found
        """
        for subclass in cls.__subclasses__():
            if name == subclass.__name__:
                return subclass
        raise ValueError(f"No subclass with name {name}")

    @classmethod
    def get_cls_name(cls: Type[C]) -> str:
        """Convenience wrapper around cls.__name__"""
        return cls.__name__

    @abstractmethod
    def get_stage_service(
        self, args: "PrivateComputationStageServiceArgs"
    ) -> "PrivateComputationStageService":
        """
        Maps StageFlow instances to StageService instances

        Arguments:
            args: Common arguments initialized in PrivateComputationService that are consumed by stage services

        Returns:
            An instantiated StageService object corresponding to the StageFlow enum member caller.

        Raises:
            NotImplementedError: The subclass doesn't implement a stage service for a given StageFlow enum member
        """
        raise NotImplementedError(
            f"get_stage_service not implemented for {self.__class__}"
        )
