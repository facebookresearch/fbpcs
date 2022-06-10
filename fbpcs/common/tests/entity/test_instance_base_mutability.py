# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from dataclasses import dataclass
from typing import List, Optional, Tuple

from fbpcs.common.entity.exceptions import InstanceFrozenFieldError

from fbpcs.common.entity.instance_base import (
    immutable_field,
    InstanceBase,
    mutable_field,
)


def create_new_num_list() -> List[int]:
    return [1, 2, 3]


@dataclass
class DummyInstance(InstanceBase):
    """
    Dummy instance class to be used in unit tests for mutability.

    Most test fields used here represent one of a kind (each of their Field objects input is unique).
    """

    instance_id: str = immutable_field()
    name: str = mutable_field()
    owner: Optional[str] = immutable_field()
    user: Optional[str] = mutable_field()
    counter: Optional[int] = immutable_field()
    number: Optional[int] = mutable_field()

    container: int = immutable_field(default=123)
    status1: str = mutable_field(default="start")
    location: str = immutable_field(default="Seattle")
    location1: str = immutable_field(default=None)
    counters: Optional[List[int]] = immutable_field(default_factory=list)
    counters1: Optional[List[int]] = mutable_field(default_factory=list)

    org: str = immutable_field(default="Measurement", init=False)
    org_id: int = immutable_field(default=9527, init=False)
    status2: str = mutable_field(default="start", init=False)
    containers: Optional[List[int]] = immutable_field(
        default_factory=create_new_num_list, init=False
    )
    containers1: Optional[List[int]] = mutable_field(
        default_factory=create_new_num_list, init=False
    )
    # this field will not be initialized when creating object
    number1: int = immutable_field(init=False)

    def get_instance_id(self) -> str:
        return self.instance_id


class TestInstanceBase(unittest.TestCase):
    def setUp(self) -> None:
        self.obj_1 = DummyInstance("1", "Tupper", None, "PCI", None, None)
        self.obj_2 = DummyInstance("2", "Tupper", "PCI", None, 800, 501, 124)

    def test_mutable(self) -> None:
        mutable_data_obj_1 = (  # (feild, original_val, change_vals)
            ["name", "Tupper", ["ECS"]],
            ["user", "PCI", ["PCA"]],
            ["number", None, [501]],
            ["status1", "start", ["completed"]],
            ["counters1", [], [[1, 2, 3], [9, 2, 3]]],
            ["status2", "start", ["completed"]],
            ["containers1", [1, 2, 3], [[9, 2, 3]]],
        )
        mutable_data_obj_2 = (  # (feild, original_val, change_vals)
            ["user", None, ["PCA", "PCI"]],
            ["number", 501, [502]],
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

    def test_immutable(self) -> None:
        immutable_data_obj_1 = (  # (feild, original_val, change_vals)
            ["instance_id", "1", "2"],
            ["owner", None, "Facebook"],
            ["counter", None, [801, 900]],
            ["container", 123, 125],
            ["location", "Seattle", "Kirkland"],
            ["location1", None, "Seattle"],
            ["counters", [], [1, 2, 3]],
            ["org", "Measurement", "signal"],
            ["org_id", 9527, 89757],
            ["containers", [1, 2, 3], [9, 2, 3]],
        )
        immutable_data_obj_2 = (  # (feild, original_val, change_vals)
            ["owner", "PCI", "Meta"],
            ["counter", 800, 900],
            ["container", 124, 125],
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

    def test_immutable_reference_type(self) -> None:
        # we can still modify the content of an immutable field if it is a reference type
        self.assertEqual(self.obj_2.containers, [1, 2, 3])
        # pyre-fixme Undefined attribute [16]: Optional has no attribute append
        self.obj_2.containers.append(4)
        self.assertEqual(self.obj_2.containers, [1, 2, 3, 4])

    def test_immutable_non_init(self) -> None:
        # this test used to test immutable fields which are not initialized when creating obj
        with self.assertRaises(AttributeError):
            # pyer-ignore [B009]
            self.obj_1.__getattribute__("number1")
        self.obj_1.number1 = 10
        self.assertEqual(self.obj_1.number1, 10)
        with self.assertRaises(InstanceFrozenFieldError):
            # pyer-ignore [B009]
            self.obj_1.__setattr__("number1", 20)
