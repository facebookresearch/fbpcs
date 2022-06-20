# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass, field
from typing import Optional, Tuple

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin, HookEventType
from fbpcs.common.entity.exceptions import InstanceFrozenFieldError, OutOfRangeHookError
from fbpcs.common.entity.frozen_field_hook import FrozenFieldHook
from fbpcs.common.entity.generic_hook import GenericHook
from fbpcs.common.entity.instance_base import (
    immutable_field,
    InstanceBase,
    mutable_field,
)
from fbpcs.common.entity.instance_base_config import InstanceBaseMetadata
from fbpcs.common.entity.range_hook import RangeHook

from fbpcs.common.entity.update_other_field_hook import UpdateOtherFieldHook

"""
This file is used to test various hooks and field mutabilities in one object.
"""

##########################
# update hooks: initialize name when id is initialized, they are both immutable
#               initialize org when user is initialized, they are both immutable
#               update output_path and storage when input_path is initialized/changed
##########################

# create a hook obj
# initialize name when id is initialized, they are both immutable
name_init_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "name",
    lambda obj: "Tupper" + obj.get_instance_id(),
    triggers=[HookEventType.POST_INIT],
)

# create a hook obj
# initialize org when user is initialized, they are both immutable
org_init_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "org",
    lambda obj: "Measurement_" + obj.user,
    triggers=[HookEventType.POST_INIT],
)

# create a hook obj
# update output_path when input_path is initialized/changed
ouput_update_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "output_path",
    lambda obj: obj.input_path + ":output",
)

# create a hook obj
# update storage when input_path is initialized/changed
storage_update_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "storage",
    lambda obj: obj.input_path + ":storage",
)


##########################
# frozen hooks: frozen input, output when status is complete
#               frozen location when region is delete
##########################

# create a hook obj
# frozen input when status is complete
frozen_input_hook: FrozenFieldHook = FrozenFieldHook(
    "input_path", lambda obj: obj.status == "complete"
)

# create a hook obj
# frozen output when status is complete
frozen_output_hook: FrozenFieldHook = FrozenFieldHook(
    "output_path", lambda obj: obj.status == "complete"
)


def region_is_deleted(obj: InstanceBase) -> bool:
    try:
        # pyre-ignore Undefined attribute [16]
        obj.region
    except AttributeError:
        return True
    else:
        return False


# create a hook obj
# frozen location when region is deleted
frozen_location_hook: FrozenFieldHook = FrozenFieldHook(
    "location",
    region_is_deleted,
)

##########################
# range hooks: number, counter, pressure, priority(immutable)
##########################

# create a hook obj
# range hook for number: [1, inf)
number_range_hook: RangeHook = RangeHook("number", 1)

# create a hook obj
# range hook for counter: [1, 10]
counter_range_hook: RangeHook = RangeHook("counter", 1, 10)

# create a hook obj
# range hook for pressure: (-inf, 100]
pressure_range_hook: RangeHook = RangeHook("pressure", None, 100)

# create a hook obj
# range hook for priority: [1, 5]
priority_range_hook: RangeHook = RangeHook("priority", 1, 5)


##########################
# generic hooks: track the highest pressure
##########################

# keep track of the highest pressure
def set_highest_pressure(obj: InstanceBase) -> None:
    # pyre-ignore Undefined attribute [16]: `InstanceBase` has no attribute `pressure`
    pressure: int = obj.pressure
    # pyre-ignore Undefined attribute [16]: `InstanceBase` has no attribute `highest_pressure`
    obj.highest_pressure = pressure


def is_highest_pressure(obj: InstanceBase) -> bool:
    highest_pressure: Optional[int] = getattr(obj, "highest_pressure", None)
    # pyre-ignore Undefined attribute [16]: `InstanceBase` has no attribute `pressure`
    pressure: int = obj.pressure
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

    instance_id: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(name_init_hook),
            **InstanceBaseMetadata.IMMUTABLE.value,
        }
    )

    input_path: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(ouput_update_hook, storage_update_hook),
            **InstanceBaseMetadata.MUTABLE.value,
        }
    )

    user: str = field(
        metadata={
            **InstanceBaseMetadata.IMMUTABLE.value,
            **DataclassHookMixin.get_metadata(org_init_hook),
        }
    )
    owner: Optional[str] = immutable_field()
    region: Optional[str] = field(
        metadata={
            **InstanceBaseMetadata.MUTABLE.value,
            **DataclassHookMixin.get_metadata(frozen_location_hook),
        },
    )
    location: Optional[str] = mutable_field()

    # an attribute is mutable by default
    number: int = field(metadata=DataclassHookMixin.get_metadata(number_range_hook))

    counter: int = field(
        metadata={
            **InstanceBaseMetadata.MUTABLE.value,
            **DataclassHookMixin.get_metadata(counter_range_hook),
        }
    )

    pressure: int = field(
        metadata={
            **InstanceBaseMetadata.MUTABLE.value,
            **DataclassHookMixin.get_metadata(
                pressure_range_hook, highest_pressure_hook
            ),
        }
    )

    priority: int = field(
        default=1,
        metadata={
            **InstanceBaseMetadata.IMMUTABLE.value,
            **DataclassHookMixin.get_metadata(priority_range_hook),
        },
    )

    status: str = field(
        default="start",
        metadata={
            **DataclassHookMixin.get_metadata(frozen_input_hook, frozen_output_hook),
            **InstanceBaseMetadata.MUTABLE.value,
        },
    )

    name: str = immutable_field(
        init=False,
    )
    org: str = immutable_field(init=False)
    output_path: str = mutable_field(init=False)
    storage: str = mutable_field(init=False)
    highest_pressure: int = mutable_field(init=False)

    def get_instance_id(self) -> str:
        return self.instance_id


class TestUpdateOtherFieldHook(unittest.TestCase):
    def setUp(self) -> None:
        # create an obj
        self.obj_1 = DummyInstance(
            "01", "//fbsource", "Meta", "PCI", None, "Seattle", 6, 7, 8
        )
        self.obj_2 = DummyInstance(
            "02", "//fbsource", "Meta", "PCI", "west-1", None, 6, 7, 8, 3
        )

    def test(self) -> None:
        self._init_event_hook()
        self._mutability()
        self._immutability()
        self._update_event_hook()
        self._delete_event_hook()

    def _mutability(self) -> None:
        mutable_data_obj_1 = (  # (feild, original_val, change_vals)
            ("input_path", "//fbsource", ("//fbcode", "//www")),
            ("region", None, ("west-1", "west-2")),
            ("location", "Seattle", ("New castle", "Pike")),
            ("number", 6, (14, 18, 17)),
            ("counter", 7, (4, 8, 7)),
            ("pressure", 8, (24, 28, 27)),
            ("status", "start", ("process", "mid", "end")),
        )
        mutable_data_obj_2 = (  # (feild, original_val, change_vals)
            ("region", "west-1", (None, "west-2")),
            ("location", None, ("New castle", "Pike")),
        )
        self._test_mutable_helper(self.obj_1, mutable_data_obj_1)
        self._test_mutable_helper(self.obj_2, mutable_data_obj_2)

    def _test_mutable_helper(
        self, intance_base_obj: InstanceBase, mutable_data: Tuple
    ) -> None:
        for test_field, original_val, change_vals in mutable_data:
            with self.subTest("Testing mutability for: ", test_field=test_field):
                # assert original
                self.assertEqual(getattr(intance_base_obj, test_field), original_val)
                # in-order to setattr
                for change_val in change_vals:
                    setattr(intance_base_obj, test_field, change_val)
                    # check val
                    self.assertEqual(getattr(intance_base_obj, test_field), change_val)

    def _immutability(self) -> None:
        immutable_data_obj_1 = (  # (feild, original_val, change_vals)
            ("instance_id", "01", "02"),
            ("user", "Meta", "AWS"),
            ("owner", "PCI", "PCS"),
            ("priority", 1, 3),
            ("name", "Tupper01", "Tupper03"),
            ("org", "Measurement_Meta", "signal"),
        )
        immutable_data_obj_2 = (  # (feild, original_val, change_vals)
            ("priority", 3, 4),
        )
        self._test_immutable_helper(self.obj_1, immutable_data_obj_1)
        self._test_immutable_helper(self.obj_2, immutable_data_obj_2)

    def _test_immutable_helper(
        self, intance_base_obj: InstanceBase, immutable_data: Tuple
    ) -> None:
        for test_field, original_val, change_vals in immutable_data:
            with self.subTest("Testing immutability for: ", test_field=test_field):
                # assert original
                self.assertEqual(getattr(intance_base_obj, test_field), original_val)
                # re-setattr will raise error
                with self.assertRaises(InstanceFrozenFieldError):
                    setattr(intance_base_obj, test_field, change_vals)

    def _init_event_hook(self) -> None:

        ##########################
        # update hooks: initialize name when id is initialized, they are both immutable
        #               initialize org when user is initialized, they are both immutable
        #               update output_path and storage when input_path is changed
        ##########################

        # assert name will be automatically initialized by id hook
        self.assertEqual(self.obj_1.name, "Tupper01")
        # assert org will be automatically initialized by user hook
        self.assertEqual(self.obj_1.org, "Measurement_Meta")
        # assert output_path will be automatically initialized by input_path hook
        self.assertEqual(self.obj_1.output_path, "//fbsource:output")
        # assert storage will be automatically initialized by input_path hook
        self.assertEqual(self.obj_1.storage, "//fbsource:storage")

        ##########################
        # range hooks: number, counter, pressure, priority
        ##########################

        # the input -5 is beyond range of number: [1, inf)
        # -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01", "//fbsource", "Meta", "PCI", "west-1", "Seattle", -5, 1, 7, 1
            )
        # the input 16 is beyond range of counter: [1, 10]
        # -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01", "//fbsource", "Meta", "PCI", "west-1", "Seattle", 5, 16, 57, 3
            )
        # the input 107 is beyond range of pressure: (-inf, 100]
        # -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01", "//fbsource", "Meta", "PCI", "west-1", "Seattle", 5, 6, 107, 2
            )
        # the input 8 is beyond range of priority: [1, 5]
        # -> range hook will be triggered
        with self.assertRaises(OutOfRangeHookError):
            DummyInstance(
                "01", "//fbsource", "Meta", "PCI", "west-1", "Seattle", 5, 6, 107, 8
            )

    def _update_event_hook(self) -> None:

        ##########################
        # update hooks: update output_path and storage when input_path is changed
        ##########################

        # change input path
        self.obj_1.input_path = "//www"

        # assert output have been changed by hook
        self.assertEqual(self.obj_1.output_path, "//www:output")
        # assert storage have been changed by hook
        self.assertEqual(self.obj_1.storage, "//www:storage")

        ##########################
        # frozen hooks: frozen input_path and output_path when status is complete
        ##########################

        # input_path and output_path are mutable now
        self.obj_1.input_path = "//fbcode"
        self.obj_1.output_path = "//fbsource:output"

        # change the status
        self.obj_1.status = "complete"

        # assert input is frozen when status is complete
        with self.assertRaises(InstanceFrozenFieldError):
            self.obj_1.input_path = "//www"
        # assert output is frozen when status is complete
        with self.assertRaises(InstanceFrozenFieldError):
            self.obj_1.output_path = "//www:output"

        ##########################
        # range hooks: number, counter, pressure, priority
        ##########################

        # we can only change number field in range [1, inf)
        self.obj_1.number = 28
        with self.assertRaises(OutOfRangeHookError):
            self.obj_1.number = -18

        # we can only change counter field in range [1, 10]
        self.obj_1.counter = 8
        with self.assertRaises(OutOfRangeHookError):
            self.obj_1.counter = 18

        # we can only change pressure field in range (-inf, 100]
        self.obj_1.pressure = -98
        with self.assertRaises(OutOfRangeHookError):
            self.obj_1.pressure = 218

        # we cannot change priority -> priority is immutable
        with self.assertRaises(InstanceFrozenFieldError):
            self.obj_1.priority = 2

        ##########################
        # generic hooks: track the highest pressure
        ##########################

        # up to now, the highest pressure is 28
        self.assertEqual(self.obj_1.highest_pressure, 28)

        self.obj_1.pressure = 70
        self.assertEqual(self.obj_1.highest_pressure, 70)
        self.obj_1.pressure = 5
        self.assertEqual(self.obj_1.highest_pressure, 70)

    def _delete_event_hook(self) -> None:

        ##########################
        # frozen hooks: frozen location when region is deleted
        ##########################

        # location is mutable now
        self.obj_1.location = "Kirkland"

        # delete region
        del self.obj_1.region

        with self.assertRaises(AttributeError):
            self.obj_1.region

        # assert location is frozen when region is deleted
        with self.assertRaises(InstanceFrozenFieldError):
            self.obj_1.location = "Bellevue"
