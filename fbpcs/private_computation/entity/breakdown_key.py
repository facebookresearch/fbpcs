#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass, fields
from typing import Any, Dict

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class BreakdownKey:
    cell_id: int
    objective_id: int
    instance_id: str

    @classmethod
    def get_default_key(cls) -> "BreakdownKey":
        return cls(**cls.get_field_names_and_default_values())

    @staticmethod
    def get_field_names_and_default_values() -> Dict[str, Any]:
        # field.type returns the type of the field (str, int, etc) and then
        # calling that result via (), eg str(), int(), etc returns the default value
        # of that type. Works with the python built in types.
        return {field.name: field.type() for field in fields(BreakdownKey)}

    def __str__(self) -> str:
        # pyre-ignore
        return self.to_json()
