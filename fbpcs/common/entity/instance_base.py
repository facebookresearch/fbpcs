#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from typing import Type, TypeVar

from dataclasses_json import DataClassJsonMixin

T = TypeVar("T", bound="InstanceBase")


class InstanceBase(DataClassJsonMixin):
    @abc.abstractmethod
    def get_instance_id(self) -> str:
        pass

    def __str__(self) -> str:
        return self.to_json()

    def dumps_schema(self) -> str:
        return self.schema().dumps(self)

    @classmethod
    def loads_schema(cls: Type[T], json_schema_str: str) -> T:
        return cls.schema().loads(json_schema_str, many=None)
