# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Set, Type, TYPE_CHECKING, Union

from dataclasses_json import config, dataclass_json, DataClassJsonMixin

# this import statument can avoid circular import
if TYPE_CHECKING:

    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )

import os

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin, HookEventType
from fbpcs.common.entity.dataclasses_mutability import (
    DataclassMutabilityMixin,
    immutable_field,
    MutabilityMetadata,
)
from fbpcs.common.entity.frozen_field_hook import FrozenFieldHook
from fbpcs.common.entity.generic_hook import GenericHook
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.common.entity.update_generic_hook import UpdateGenericHook
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
)
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.constants import FBPCS_BUNDLE_ID
from marshmallow import fields
from marshmallow_enum import EnumField


class PrivateComputationRole(Enum):
    PUBLISHER = "PUBLISHER"
    PARTNER = "PARTNER"


class PrivateComputationGameType(Enum):
    LIFT = "LIFT"
    ATTRIBUTION = "ATTRIBUTION"


UnionedPCInstance = Union[
    PIDInstance, PCSMPCInstance, PostProcessingInstance, StageStateInstance
]


@dataclass_json
@dataclass
class StatusUpdate:
    status: PrivateComputationInstanceStatus
    status_update_ts: int


# called in post_status_hook
# happens whenever status is updated
def post_update_status(obj: "InfraConfig") -> None:
    # TODO:T126122461 uniform time assignment for `status_update_ts` and `end_ts`
    obj.status_update_ts = int(datetime.now(tz=timezone.utc).timestamp())
    append_status_updates(obj)
    if obj.is_stage_flow_completed():
        obj.end_ts = int(time.time())


# called in post_status_hook
def append_status_updates(obj: "InfraConfig") -> None:
    pair: StatusUpdate = StatusUpdate(
        obj.status,
        obj.status_update_ts,
    )
    obj.status_updates.append(pair)


# create update_generic_hook for status
post_status_hook: UpdateGenericHook["InfraConfig"] = UpdateGenericHook(
    triggers=[HookEventType.POST_UPDATE],
    update_function=post_update_status,
)


# create FrozenFieldHook: set end_ts immutable after initialized
set_end_ts_immutable_hook: FrozenFieldHook = FrozenFieldHook(
    other_field="end_ts",
    freeze_when=lambda obj: obj.end_ts != 0,
)


# called in num_pid_mpc_containers_hook
def raise_containers_error(obj: "InfraConfig") -> None:
    raise ValueError(
        f"num_pid_containers must be less than or equal to num_mpc_containers. Received num_pid_containers = {obj.num_pid_containers} and num_mpc_containers = {obj.num_mpc_containers}"
    )


# called in num_pid_mpc_containers_hook
def not_valid_containers(obj: "InfraConfig") -> bool:
    if hasattr(obj, "num_pid_containers") and hasattr(obj, "num_mpc_containers"):
        return obj.num_pid_containers > obj.num_mpc_containers
    # one or both not initialized yet
    return False


# create generic_hook for num_pid_containers > num_mpc_containers check
# if num_pid_containers < num_mpc_containers => raise an error
num_pid_mpc_containers_hook: GenericHook["InfraConfig"] = GenericHook(
    hook_function=raise_containers_error,
    triggers=[HookEventType.POST_INIT, HookEventType.POST_UPDATE],
    hook_condition=not_valid_containers,
)


@dataclass
class InfraConfig(DataClassJsonMixin, DataclassMutabilityMixin):
    """Stores metadata of infra config in a private computation instance

    Public attributes:
        instance_id: this is unique for each PrivateComputationInstance.
                        It is used to find and generate PCInstance in json repo.
        role: an Enum indicating if this PrivateComputationInstance is a publisher object or partner object
        status: an Enum indecating what stage and status the PCInstance is currently in
        status_update_ts: the time of last status update
        instances: during the whole computation run, all the instances created will be sotred here.
        game_type: an Enum indicating if this PrivateComputationInstance is for private lift or private attribution
        num_pid_containers: the number of containers used in pid
        num_mpc_containers: the number of containers used in mpc
        num_files_per_mpc_container: the number of files for each container
        fbpcs_bundle_id: an string indicating the fbpcs bundle id to run.
        tier: an string indicating the release binary tier to run (rc, canary, latest)
        retry_counter: the number times a stage has been retried
        creation_ts: the time of the creation of this PrivateComputationInstance
        end_ts: the time of the the end when finishing a computation run
        mpc_compute_concurrency: number of threads to run per container at the MPC compute metrics stage

    Private attributes:
        _stage_flow_cls_name: the name of a PrivateComputationBaseStageFlow subclass (cls.__name__)
    """

    instance_id: str = immutable_field()
    role: PrivateComputationRole = immutable_field()
    status: PrivateComputationInstanceStatus = field(
        metadata=DataclassHookMixin.get_metadata(post_status_hook)
    )
    status_update_ts: int
    instances: List[UnionedPCInstance]
    game_type: PrivateComputationGameType = immutable_field()

    # TODO: these numbers should be immutable eventually
    num_pid_containers: int = field(
        metadata=DataclassHookMixin.get_metadata(num_pid_mpc_containers_hook)
    )
    num_mpc_containers: int = field(
        metadata=DataclassHookMixin.get_metadata(num_pid_mpc_containers_hook)
    )
    num_files_per_mpc_container: int

    # status_updates will be update in status hook
    status_updates: List[StatusUpdate]

    fbpcs_bundle_id: Optional[str] = immutable_field(init=False)
    tier: Optional[str] = immutable_field(default=None)
    pcs_features: Set[PCSFeature] = field(
        default_factory=set,
        metadata={
            # this makes type warning away when serialize this field
            **config(mm_field=fields.List(EnumField(PCSFeature))),
            **MutabilityMetadata.IMMUTABLE.value,
        },
    )
    pce_config: Optional[PCEConfig] = None

    # stored as a string because the enum was refusing to serialize to json, no matter what I tried.
    # TODO(T103299005): [BE] Figure out how to serialize StageFlow objects to json instead of using their class name
    _stage_flow_cls_name: str = immutable_field(default="PrivateComputationStageFlow")

    retry_counter: int = 0
    creation_ts: int = immutable_field(default_factory=lambda: int(time.time()))

    end_ts: int = field(
        default=0, metadata=DataclassHookMixin.get_metadata(set_end_ts_immutable_hook)
    )

    # TODO: concurrency should be immutable eventually
    mpc_compute_concurrency: int = 1

    @property
    def stage_flow(self) -> Type["PrivateComputationBaseStageFlow"]:
        # this inner-function import allow us to call PrivateComputationBaseStageFlow.cls_name_to_cls
        # TODO: [BE] create a safe way to avoid inner-function import
        from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
            PrivateComputationBaseStageFlow,
        )

        return PrivateComputationBaseStageFlow.cls_name_to_cls(
            self._stage_flow_cls_name
        )

    def is_stage_flow_completed(self) -> bool:
        return self.status is self.stage_flow.get_last_stage().completed_status

    def __post_init__(self):
        # ensure mutability before override __post_init__
        super().__post_init__()
        # note: The reason can't make it fbpcs_bundle_id = immutable_field(default=os.getenv(FBPCS_BUNDLE_ID)),
        # is because that will happend in static varible when module been loaded, moved it to __post_init__ for better init control
        self.fbpcs_bundle_id = os.getenv(FBPCS_BUNDLE_ID)
