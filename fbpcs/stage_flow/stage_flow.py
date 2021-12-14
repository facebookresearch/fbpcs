#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from dataclasses import dataclass
from enum import Enum, EnumMeta
from functools import cached_property
from typing import Any, Dict, Tuple
from typing import Optional, Generic, Type, TypeVar

from fbpcs.stage_flow.exceptions import StageFlowStageNotFoundError

# C  -> Class
C = TypeVar("C", bound="StageFlow")
Status = TypeVar("Status")


@dataclass(frozen=True)
class StageFlowData(Generic[Status]):
    """Store data used when to determine how to flow between stages.

    This class is used by StageFlow and its subclasses to generate maps between
    stage -> status and status -> stage. Additional relevant information used to
    determine when to transition stages can be put in subclasses of StageFlowData.

    For example, in private computation, some stages are "joint stages," meaning
    that two parties run together. The partner must wait for the publisher to finish
    its stage before the partner can run its stage. You could subclass this dataclass
    and add an is_joint_stage bool and tag all of the StageFlow enum members with whether
    or not the stage is a joint stage. Then, you can use that information to determine if
    you can run/move to the next stage.
    """

    started_status: Status
    completed_status: Status
    failed_status: Status


class StageFlowMeta(EnumMeta):
    """
    Metaclass intended to be used by the StageFlow enum. It overrides the
    repr dunder method to provide a pretty representation of a stage flow
    """

    def __init__(
        self, name: str, bases: Tuple[Type[Any], ...], namespace: Dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)
        self._stage_flow_pretty: str = " -> ".join(self._member_names_)

    def __repr__(self) -> str:
        """Used to pretty print stage flows, e.g. stage1 -> stage2 -> stage3"""
        return self._stage_flow_pretty

    def __getitem__(self: Type[C], item: str) -> C:
        try:
            return super().__getitem__(item)
        except KeyError:
            pass

        try:
            return super().__getitem__(item.upper())
        except KeyError:
            raise StageFlowStageNotFoundError(
                f"{item} is not a stage in {self.__name__}. Valid stages: {self!r}"
            ) from None


class StageFlow(Enum, metaclass=StageFlowMeta):
    """Enumerates the stages in an app and provides methods to help it transition between them.

    StageFlow enum allows api consumers to enumerate stages an application will run,
    along with any relevant data used to determine when transitions can be made.
    Consumers provide the order that the stages occur in and the class magically
    provides methods to tell you how to move between the stages and how to convert between stages and status.
    Stages can be added and removed with no change to application code. This allows consumers to easily
    build state machines capable of running different games and helps provide a safe way to test
    new game flows without disrupting the official production flow.

    By default, all StageFlows must provide a start, completed, and failed status per Stage.
    This is used to create a bidirectional mapping between application instance state and Stage,
    which allows applications to easily determine where they are in a run.

    To use StageFlows, you need to define two enums:

    * An enum listing possible statuses
    * The StageFlow enum listing stage names -> statuses

    This would replace the following steps, based on how we have historically managed stage transitions:

    * Define an enum listing statuses
    * Define an enum listing stage names
    * Create a dictionary mapping from stage name to statuses
    * Create a dictionary mapping from statuses to stage name
    * Create a dictionary mapping from stage to next and previous stages
    * If you add or remove stages, you must manually edit the mappings between stages, else
    your application would break
    * Hardcode a list of all possible start statuses if you want to support dry runs

    tl;dr, StageFlows significantly reduces hardcoding and manual/duplicate stage management logic
    by automating stage <-> statuses and stage <-> stage mappings.

    Beyond the bare minimum functionality, stage flow provides nice things for free:
    * Generates a set containing all start status at class creation time
    * Uses custom metaclass to pretty print flows, where each stage is separated by an arrow
        e.g. stage1 -> stage2 -> stage3
    * Implements custom repr in class to pretty print flow and points to which stage is running.
        e.g. stage1 -> [**stage2**] -> stage3


    Private Attributes:
        _stage_flow_started_statuses: set containing all start statuses defined in the flow
    """

    def __init_subclass__(cls: Type[C]) -> None:
        """Post hook ran after class instantiation. Initialize the started status map."""
        super().__init_subclass__()
        cls._stage_flow_started_statuses = set()
        cls._stage_flow_completed_statuses = set()
        cls._stage_flow_failed_statuses = set()

    def __new__(cls: Type[C], data: StageFlowData[Status]) -> C:
        """Override instance creation to map from status -> stage and add start statuses to set"""
        member = object.__new__(cls)
        member._value_ = data

        cls._value2member_map_[data.started_status] = member
        cls._value2member_map_[data.completed_status] = member
        cls._value2member_map_[data.failed_status] = member

        if data.started_status:
            cls._stage_flow_started_statuses.add(data.started_status)
        if data.completed_status:
            cls._stage_flow_completed_statuses.add(data.completed_status)
        if data.failed_status:
            cls._stage_flow_failed_statuses.add(data.failed_status)

        return member

    def __repr__(self) -> str:
        """Used to pretty print stage flows, e.g. stage1 -> [**stage2**] -> stage3"""
        names = self.__class__._member_names_.copy()
        pos = names.index(self.name)

        names[pos] = f"[**{self.name}**]"
        return " -> ".join(names)

    @classmethod
    def get_stage_from_status(cls: Type[C], status: Status) -> C:
        """Convert a status to the associated StageFlow member

        Args:
            status: The status being mapped to a StageFlow member

        Returns:
            return: StageFlow member if there is a runnable stage, None otherwise

        Raises:
            ValueError: when status cannot be mapped to any stage in the StageFlow
        """
        if status not in cls._value2member_map_:
            raise ValueError(f"{status} is not a possible status for {cls.__name__}")
        stage = cls._value2member_map_[status]
        # To appease pyre
        assert isinstance(stage, cls)
        return stage

    @classmethod
    def get_next_runnable_stage_from_status(
        cls: Type[C], status: Status
    ) -> Optional[C]:
        """Convert a status to the next runnable stage

        Args:
            status: The status being mapped to a StageFlow member

        Returns:
            return: StageFlow member if there is a runnable stage, None otherwise

        Raises:
            ValueError: when status cannot be mapped to any stage in the StageFlow
        """
        stage = cls.get_stage_from_status(status)

        # If the current stage is completed, get the next one
        if status is stage.value.completed_status:
            return stage.next_stage
        # if the current stage is failed, we should try again
        elif status is stage.value.failed_status:
            return stage
        # if it's a start status, then you shouldn't run the stage
        else:
            return None

    @classmethod
    def get_first_stage(cls: Type[C]) -> C:
        return list(cls)[0]

    @classmethod
    def get_stage_from_str(cls: Type[C], stage_name: str) -> C:
        """Convert a stage name string to a stage object

        Args:
            stage_name: the name of the stage

        Returns:
            return: StageFlow member corresponding to the stage name

        Raises:
            StageFlowStageNotFoundError: the stage specified does not exist
        """
        return cls[stage_name]

    @classmethod
    def get_last_stage(cls: Type[C]) -> C:
        return list(cls)[-1]

    @classmethod
    def is_started_status(cls: Type[C], status: Status) -> bool:
        return status in cls._stage_flow_started_statuses

    @classmethod
    def is_completed_status(cls: Type[C], status: Status) -> bool:
        return status in cls._stage_flow_completed_statuses

    @classmethod
    def is_failed_status(cls: Type[C], status: Status) -> bool:
        return status in cls._stage_flow_failed_statuses

    @cached_property
    def next_stage(self: C) -> Optional[C]:
        members = list(self.__class__)
        index = members.index(self) + 1
        if index >= len(members):
            return None
        return members[index]

    @cached_property
    def previous_stage(self: C) -> Optional[C]:
        members = list(self.__class__)
        index = members.index(self) - 1
        if index < 0:
            return None
        return members[index]
