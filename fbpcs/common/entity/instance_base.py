#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Type, TypeVar

from dataclasses_json import DataClassJsonMixin
from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.exceptions import InstanceFrozenFieldError
from fbpcs.common.entity.instance_base_config import (
    InstanceBaseMetadata,
    IS_FROZEN_FIELD,
)

T = TypeVar("T", bound="InstanceBase")

# pyre-ignore Missing parameter annotation [4]
mutable_field = partial(field, metadata=InstanceBaseMetadata.MUTABLE.value)
# pyre-ignore Missing parameter annotation [4]
immutable_field = partial(field, metadata=InstanceBaseMetadata.IMMUTABLE.value)


@dataclass
class InstanceBase(DataClassJsonMixin, DataclassHookMixin):
    # this boolean will be set to True after an obj initialization
    initialized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.initialized = True

    @abc.abstractmethod
    def get_instance_id(self) -> str:
        pass

    def __str__(self) -> str:
        return self.dumps_schema()

    def dumps_schema(self) -> str:
        return self.schema().dumps(self)

    @classmethod
    def loads_schema(cls: Type[T], json_schema_str: str) -> T:
        return cls.schema().loads(json_schema_str, many=None)

    # pyre-ignore Missing parameter annotation [2]
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
