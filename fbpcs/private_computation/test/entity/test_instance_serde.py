#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest


class TestInstanceSerde(unittest.TestCase):
    """
    Some of these tests use pre-serialized instances
    as input. These will need to be updated when there
    are unavoidable breaking changes to serialization.

    To update, run buck run //fbpcs:pc_generate_instance_json
    """

    def test_pid_deserialiation(self) -> None:
        pass

    def test_mpc_deserialiation(self) -> None:
        pass

    def test_pc_deserialiation(self) -> None:
        pass

    def test_pid_serialization(self) -> None:
        pass

    def test_mpc_serialization(self) -> None:
        pass

    def test_pc_serialization(self) -> None:
        pass
