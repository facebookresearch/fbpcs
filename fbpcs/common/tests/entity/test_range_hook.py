# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass, field

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.dataclasses_mutability import MutabilityMetadata
from fbpcs.common.entity.exceptions import MissingRangeHookError, OutOfRangeHookError
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.range_hook import RangeHook

# create a hook obj
# range hook for number: [1, inf)
number_range_hook: RangeHook = RangeHook("number", 1)

# create a hook obj
# range hook for counter: [1, 10]
counter_range_hook: RangeHook = RangeHook("counter", 1, 10)

# create a hook obj
# range hook for pressure: (-inf, 100]
pressure_range_hook: RangeHook = RangeHook("pressure", None, 100)


@dataclass
class DummyInstance(InstanceBase):
    """
    Dummy instance class to be used in unit tests.

    """

    instance_id: str
    name: str

    number: int = field(metadata=DataclassHookMixin.get_metadata(number_range_hook))

    # we can specify mutability
    # each field will be mutable as default
    counter: int = field(
        metadata={
            **MutabilityMetadata.MUTABLE.value,
            **DataclassHookMixin.get_metadata(counter_range_hook),
        }
    )

    pressure: int = field(
        metadata={
            **MutabilityMetadata.MUTABLE.value,
            **DataclassHookMixin.get_metadata(pressure_range_hook),
        }
    )

    def get_instance_id(self) -> str:
        return self.instance_id


class TestUpdateOtherFieldHook(unittest.TestCase):
    def test_create_range_field_hook(self) -> None:
        with self.assertRaises(MissingRangeHookError):
            RangeHook("dummy", None, None)

    def test_init_event_out_of_range_field_hook(self) -> None:
        # the input -5 is beyond range of number -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01",
                "Tupper01",
                -5,
                1,
                7,
            )
        # the input 16 is beyond range of counter -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01",
                "Tupper01",
                5,
                16,
                7,
            )
        # the input 107 is beyond range of pressure -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01",
                "Tupper01",
                5,
                6,
                107,
            )

    def test_init_event_range_field_hook(self) -> None:
        # the input 100 is in range of number -> [1, inf)
        number_hook = DummyInstance(
            "01",
            "Tupper01",
            100,
            1,
            7,
        )
        number_hook.number = 28

        # the input 9 is in range of counter -> [1, 10]
        counter_hook = DummyInstance(
            "01",
            "Tupper01",
            5,
            9,
            7,
        )
        counter_hook.counter = 8

        # the input -80 is in range of pressure -> (-inf, 100]
        pressure_hook = DummyInstance(
            "01",
            "Tupper01",
            5,
            6,
            -80,
        )
        pressure_hook.pressure = -98

    def test_init_and_update_event_range_field_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance(
            "01",
            "Tupper01",
            5,
            6,
            7,
        )

        # we can change number field in range [1, inf)
        dummy_obj.number = 28
        with self.assertRaises(OutOfRangeHookError):
            dummy_obj.number = -18

        # we can change counter field in range [1, 10]
        dummy_obj.counter = 8
        with self.assertRaises(OutOfRangeHookError):
            dummy_obj.counter = 18

        # we can change pressure field in range (-inf, 100]
        dummy_obj.pressure = -98
        with self.assertRaises(OutOfRangeHookError):
            dummy_obj.pressure = 218
