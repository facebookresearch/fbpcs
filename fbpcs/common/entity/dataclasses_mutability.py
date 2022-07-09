# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import Any, TypeVar

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.exceptions import InstanceFrozenFieldError


T = TypeVar("T")

IS_FROZEN_FIELD: str = "mutability"


class MutabilityMetadata(Enum):
    MUTABLE = {IS_FROZEN_FIELD: False}
    IMMUTABLE = {IS_FROZEN_FIELD: True}


mutable_field = partial(field, metadata=MutabilityMetadata.MUTABLE.value)
immutable_field = partial(field, metadata=MutabilityMetadata.IMMUTABLE.value)


@dataclass
class DataclassMutabilityMixin(DataclassHookMixin):
    """
    You also get hooks if inherits this mutability mixin
    """

    # this boolean will be set to True after an obj initialization
    initialized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.initialized = True

    def __setattr__(self, name: str, value: Any) -> None:
        # if setattr is called after initialization
        if self.initialized:
            # if we cannot find it, this field has not been initialized yet
            try:
                self.__getattribute__(name)
            except AttributeError:
                DataclassHookMixin.__setattr__(self, name, value)
            else:
                # if this field has been initialized and it is immutable
                # pyre-fixme Undefined attribute [16]: InstanceBase has no attribute __dataclass_fields__
                if self.__dataclass_fields__[name].metadata.get(IS_FROZEN_FIELD, False):
                    raise InstanceFrozenFieldError(name)
                else:
                    DataclassHookMixin.__setattr__(self, name, value)
        else:
            # if setattr is called during initialization
            DataclassHookMixin.__setattr__(self, name, value)
