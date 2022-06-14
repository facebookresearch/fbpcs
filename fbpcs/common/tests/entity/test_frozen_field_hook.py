# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass, field

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.exceptions import InstanceFrozenFieldError
from fbpcs.common.entity.frozen_field_hook import FrozenFieldHook
from fbpcs.common.entity.instance_base import InstanceBase, mutable_field
from fbpcs.common.entity.instance_base_config import InstanceBaseMetadata

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

# create a hook obj
# frozen location when user is deleted
frozen_location_hook: FrozenFieldHook = FrozenFieldHook(
    "location", lambda obj: getattr(obj, "user", None) is None
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

    user: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(frozen_location_hook),
            **InstanceBaseMetadata.IMMUTABLE.value,
        },
    )
    location: str = mutable_field()

    status: str = field(
        default="start",
        metadata={
            **DataclassHookMixin.get_metadata(frozen_input_hook, frozen_output_hook),
            **InstanceBaseMetadata.MUTABLE.value,
        },
    )

    def get_instance_id(self) -> str:
        return self.instance_id


class TestFrozenFieldHook(unittest.TestCase):
    def setUp(self) -> None:
        # create an obj
        self.dummy_obj = DummyInstance(
            "01",
            "Tupper01",
            "//fbsource",
            "//fbsource:output",
            "//fbsource:storage",
            "Meta",
            "Seattle",
        )

    def test_update_event_frozen_field_hook(self) -> None:
        # input_path and output_path are mutable now
        self.dummy_obj.input_path = "//fbcode"
        self.dummy_obj.output_path = "//fbsource:output"

        # change the status
        self.dummy_obj.status = "complete"

        # assert input is frozen when status is complete
        with self.assertRaises(InstanceFrozenFieldError):
            self.dummy_obj.input_path = "//www"
        # assert output is frozen when status is complete
        with self.assertRaises(InstanceFrozenFieldError):
            self.dummy_obj.output_path = "//www:output"

    def test_delete_event_frozen_field_hook(self) -> None:
        # location is mutable now
        self.dummy_obj.location = "Kirkland"

        # delete user
        del self.dummy_obj.user

        with self.assertRaises(AttributeError):
            self.dummy_obj.user

        # assert location is frozen when user is deleted
        with self.assertRaises(InstanceFrozenFieldError):
            self.dummy_obj.location = "Bellevue"
