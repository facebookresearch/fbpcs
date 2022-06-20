# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from dataclasses import dataclass, field
from typing import Optional

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin, HookEventType
from fbpcs.common.entity.generic_hook import GenericHook
from fbpcs.common.entity.instance_base import InstanceBase

# keep tracking the highest pressure
def set_highest_pressure(obj: InstanceBase) -> None:
    pressure: int = getattr(obj, "pressure", None)
    # pyre-ignore Undefined attribute [16]: `InstanceBase` has no attribute `highest_pressure`
    obj.highest_pressure = pressure


def is_highest_pressure(obj: InstanceBase) -> bool:
    highest_pressure: Optional[int] = getattr(obj, "highest_pressure", None)
    pressure: int = getattr(obj, "pressure", None)
    return highest_pressure is None or highest_pressure < pressure


# create hook obj
# get the highest pressure
highest_pressure_hook: GenericHook[InstanceBase] = GenericHook(
    set_highest_pressure,
    [HookEventType.POST_INIT, HookEventType.POST_UPDATE],
    is_highest_pressure,
)


@dataclass
class DummyInstance(InstanceBase):
    """
    Dummy instance class to be used in unit tests.

    """

    instance_id: str
    name: str

    input_path: str
    output_path: str

    pressure: int = field(
        metadata=DataclassHookMixin.get_metadata(highest_pressure_hook),
    )

    highest_pressure: int = field(init=False)

    def get_instance_id(self) -> str:
        return self.instance_id


class TestFrozenFieldHook(unittest.TestCase):
    def test_init_update_events_generic_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance(
            "01", "Tupper01", "//fbsource", "//fbsource:output", 25
        )

        self.assertEqual(dummy_obj.highest_pressure, 25)

        dummy_obj.pressure = 70
        self.assertEqual(dummy_obj.highest_pressure, 70)
        dummy_obj.pressure = 50
        self.assertEqual(dummy_obj.highest_pressure, 70)
