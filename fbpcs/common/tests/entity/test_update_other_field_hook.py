# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass, field

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.instance_base import InstanceBase, mutable_field
from fbpcs.common.entity.instance_base_config import InstanceBaseMetadata

from fbpcs.common.entity.update_other_field_hook import UpdateOtherFieldHook

# create a hook obj
# update name when id is changed
name_update_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "name",
    lambda obj: "Tupper" + obj.get_instance_id(),
)

# create a hook obj
# in python, we do not need to specify the generic type when initialize an obj
# update output_path when input_path is changed
ouput_update_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "output_path",
    lambda obj: obj.input_path + ":output",
)

# create a hook obj
# update storage when input_path is changed
storage_update_hook: UpdateOtherFieldHook = UpdateOtherFieldHook(
    "storage",
    lambda obj: obj.input_path + ":storage",
)


@dataclass
class DummyInstance(InstanceBase):
    """
    Dummy instance class to be used in unit tests.

    """

    instance_id: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(name_update_hook),
            **InstanceBaseMetadata.MUTABLE.value,
        }
    )

    input_path: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(ouput_update_hook, storage_update_hook),
            **InstanceBaseMetadata.MUTABLE.value,
        }
    )

    name: str = mutable_field(init=False)
    output_path: str = mutable_field(init=False)
    storage: str = mutable_field(init=False)

    def get_instance_id(self) -> str:
        return self.instance_id


class TestUpdateOtherFieldHook(unittest.TestCase):
    def test_init_event_update_field_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance("01", "//fbsource")

        # assert name will be automatically initialized by id hook
        self.assertEqual(dummy_obj.name, "Tupper01")
        # assert output_path will be automatically initialized by instance_id hook
        self.assertEqual(dummy_obj.output_path, "//fbsource:output")
        # assert storage will be automatically initialized by id hook
        self.assertEqual(dummy_obj.storage, "//fbsource:storage")

    def test_update_event_update_field_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance("01", "//fbsource")

        # change the input path
        dummy_obj.input_path = "//www"
        dummy_obj.instance_id = "02"

        # assert name have been changed by hook
        self.assertEqual(dummy_obj.name, "Tupper02")
        # assert output have been changed by hook
        self.assertEqual(dummy_obj.output_path, "//www:output")
        # assert storage have been changed by hook
        self.assertEqual(dummy_obj.storage, "//www:storage")
