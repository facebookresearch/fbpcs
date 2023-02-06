#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

# postpone evaluation of type hint annotations until runtime (support forward reference)
# This will become the default in python 4.0: https://peps.python.org/pep-0563/
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Dict, Generic, List, Optional, Type, TYPE_CHECKING, TypeVar

from dataclasses_json import DataClassJsonMixin

# only do these imports when type checking (support forward reference)
if TYPE_CHECKING:
    from fbpcs.bolt.bolt_hook import BoltHook, BoltHookKey

from fbpcs.bolt.constants import DEFAULT_POLL_INTERVAL_SEC
from fbpcs.bolt.exceptions import IncompatibleStageError
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltCreateInstanceArgs(ABC):
    instance_id: str


T = TypeVar("T", bound=BoltCreateInstanceArgs)
U = TypeVar("U", bound=BoltCreateInstanceArgs)


@dataclass
class BoltPlayerArgs(Generic[T]):
    create_instance_args: T
    expected_result_path: Optional[str] = None


@dataclass
class BoltJob(DataClassJsonMixin, Generic[T, U]):
    job_name: str
    publisher_bolt_args: BoltPlayerArgs[T]
    partner_bolt_args: BoltPlayerArgs[U]
    poll_interval: int = DEFAULT_POLL_INTERVAL_SEC
    num_tries: Optional[int] = None

    # allows the final stage to be configured for each job to stop a run early
    # if one isn't given, final_stage defaults to the final stage of the job's stage_flow
    final_stage: Optional[PrivateComputationBaseStageFlow] = None
    # pyre doesn't accept Any or object as an option, so we will just ignore it
    # pyre-ignore: generic type BoltHook requires 1 generic parameter
    hooks: Dict["BoltHookKey", List["BoltHook"]] = field(default_factory=dict)

    def is_finished(
        self,
        publisher_status: PrivateComputationInstanceStatus,
        partner_status: PrivateComputationInstanceStatus,
        stage_flow: Type[PrivateComputationBaseStageFlow],
    ) -> bool:
        # TODO: T130069872 There is a potential risk that graph API's final_stage is RESULTS_READY, which is translated to AGGREGATION COMPLETED
        # https://fburl.com/code/j3dl2uld, if final_stage defaults to stage_flow's last stage then it will be stuck
        if (
            self.final_stage is not None
            and self.final_stage.get_cls_name() != stage_flow.get_cls_name()
        ):
            raise IncompatibleStageError(
                f"Final stage {self.final_stage} is not part of {stage_flow.get_cls_name()}"
            )

        final_status = (
            self.final_stage.completed_status
            if self.final_stage
            else stage_flow.get_last_stage().completed_status
        )
        return (publisher_status is final_status) and (partner_status is final_status)
