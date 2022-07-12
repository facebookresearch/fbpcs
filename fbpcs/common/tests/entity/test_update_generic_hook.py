# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyer-strict

import unittest
from dataclasses import dataclass

from fbpcs.common.entity.instance_base import InstanceBase


@dataclass
class DummyInstance(InstanceBase):
    """
    Dummy instance class to be used in unit tests.

    """


class TestUpdateGenericHook(unittest.TestCase):
    def test_init_event_update_generic_hook(self) -> None:
        pass

    def test_update_event_update_generic_hook(self) -> None:
        pass
