#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.test.entity.generate_instance_json import (
    LIFT_PID_PATH,
    LIFT_MPC_PATH,
    LIFT_PC_PATH,
    gen_dummy_pid_instance,
    gen_dummy_mpc_instance,
    gen_dummy_pc_instance,
)

ERR_MSG: str = (
    "Unable to deserialize instance. You may have made a breaking change. "
    "See TestInstanceSerde (test_instance_serde.py) unit test suite for "
    "more information on next steps."
)


class TestInstanceSerde(unittest.TestCase):
    """These Unit tests check if instances serialization and deserialization is still working.

    Deserialization is tested by using old, pre-serialized instance json files stored in the test_resources directory.
    If the tests are broken, you should first try to rework your approach and see if you can avoid failures. See this
    post for more information: https://fb.workplace.com/groups/164332244998024/permalink/718007142963862/.

    If you are seeing test failures, then it is very likely that your changes are going to break prod. However, if you
    followed the instructions on the post linked above to avoid breaking prod and are still experiencing failures, it is possible
    the test files haven't been updated in awhile. For example, if you are changing an optional field to be required and it is causing
    a deserialization failure, then it's likely that test files haven't been updated since the optional field is introduced.

    A couple of approaches for debugging this failure:
        1) You can check the test_resources directory to see if the field is defined in the serialized json.
        2) Stash your changes, update the test files, and then run a test. Everything should work. Then, unstash your changes
        and run the test again. If everything still works, then the test files were just out of date - you won't break prod :)

    If you tried both approaches listed above and you are still failing the test, then I'm pretty sure your change is going to
    break prod. Again, refer to the post linked above and make sure you are making changes in a safe way.

    If you need to update test files, you can run buck run //fbpcs:pc_generate_instance_json
    """

    def test_pid_deserialiation(self) -> None:
        # this tests that old fields (and instances) can be deserialized
        with open(LIFT_PID_PATH) as f:
            instance_json = f.read().strip()
        try:
            PIDInstance.loads_schema(instance_json)
        except Exception as e:
            raise RuntimeError(ERR_MSG) from e

    def test_mpc_deserialiation(self) -> None:
        # this tests that old fields (and instances) can be deserialized
        with open(LIFT_MPC_PATH) as f:
            instance_json = f.read().strip()
        try:
            PCSMPCInstance.loads_schema(instance_json)
        except Exception as e:
            raise RuntimeError(ERR_MSG) from e

    def test_pc_deserialiation(self) -> None:
        # this tests that old fields (and instances) can be deserialized
        with open(LIFT_PC_PATH) as f:
            instance_json = f.read().strip()
        try:
            PrivateComputationInstance.loads_schema(instance_json)
        except Exception as e:
            raise RuntimeError(ERR_MSG) from e

    def test_pid_serialization(self) -> None:
        # this tests that new fields can be serialized
        pid_instance = gen_dummy_pid_instance()
        pid_instance.dumps_schema()

    def test_mpc_serialization(self) -> None:
        # this tests that new fields can be serialized
        mpc_instance = gen_dummy_mpc_instance()
        mpc_instance.dumps_schema()

    def test_pc_serialization(self) -> None:
        # this tests that new fields can be serialized
        pc_instance = gen_dummy_pc_instance()
        pc_instance.dumps_schema()
