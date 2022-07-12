# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass, field

from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.dataclasses_mutability import MutabilityMetadata, mutable_field

from fbpcs.common.entity.update_generic_hook import UpdateGenericHook


def after_update_input(obj: "DummyInstance"):
    obj.output_path = obj.input_path + ":output"
    obj.storage = obj.input_path + ":storage"


# create a hook obj
# update output_path and storage in one hook when input_path is changed
input_updated_hook: UpdateGenericHook = UpdateGenericHook(after_update_input)


@dataclass
class DummyInstance(DataclassHookMixin):
    """
    Dummy instance class to be used in unit tests.

    """

    input_path: str = field(
        metadata={
            **DataclassHookMixin.get_metadata(input_updated_hook),
            **MutabilityMetadata.MUTABLE.value,
        }
    )

    output_path: str = mutable_field(init=False)
    storage: str = mutable_field(init=False)


class TestUpdateGenericHook(unittest.TestCase):
    def test_init_event_update_generic_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance("//fbsource")

        # assert output_path will be automatically initialized by instance_id hook
        self.assertEqual(dummy_obj.output_path, "//fbsource:output")
        # assert storage will be automatically initialized by id hook
        self.assertEqual(dummy_obj.storage, "//fbsource:storage")

    def test_update_event_update_generic_hook(self) -> None:
        # create an obj
        dummy_obj = DummyInstance("//fbsource")

        # change the input path
        dummy_obj.input_path = "//www"

        # assert output have been changed by hook
        self.assertEqual(dummy_obj.output_path, "//www:output")
        # assert storage have been changed by hook
        self.assertEqual(dummy_obj.storage, "//www:storage")
