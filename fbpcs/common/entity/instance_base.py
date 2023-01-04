#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from dataclasses import dataclass
from typing import Type, TypeVar

from dataclasses_json import dataclass_json, Undefined
from fbpcs.common.entity.dataclasses_mutability import DataclassMutabilityMixin

T = TypeVar("T", bound="InstanceBase")


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class InstanceBase(DataclassMutabilityMixin):
    @abc.abstractmethod
    def get_instance_id(self) -> str:
        pass

    def __str__(self) -> str:
        return self.dumps_schema()

    def dumps_schema(self) -> str:
        # pyre-ignore[16] Undefined attribute
        return self.schema().dumps(self)

    @classmethod
    def loads_schema(cls: Type[T], json_schema_str: str) -> T:
        # pyre-ignore[16] Undefined attribute
        return cls.schema().loads(json_schema_str, many=None)
