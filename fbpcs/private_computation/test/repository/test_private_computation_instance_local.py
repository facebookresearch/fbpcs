#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import random
import string
import unittest

from fbpcp.entity.mpc_instance import MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.repository.private_computation_instance_local import (
    LocalPrivateComputationInstanceRepository,
)


class TestLocalPrivateComputationInstanceRepository(unittest.TestCase):
    def setUp(self):
        instance_id = self._get_random_id()
        self.repo = LocalPrivateComputationInstanceRepository("./")
        self.test_mpc_instance = PCSMPCInstance.create_instance(
            instance_id=instance_id,
            game_name="conversion_lift",
            mpc_party=MPCParty.SERVER,
            num_workers=2,
        )

    def test_read(self):
        instance_id = self._get_random_id()
        test_read_private_computation_instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=PrivateComputationRole.PUBLISHER,
            instances=[self.test_mpc_instance],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            num_files_per_mpc_container=40,
            game_type=PrivateComputationGameType.LIFT,
        )
        self.repo.create(test_read_private_computation_instance)
        self.assertEqual(
            self.repo.read(instance_id), test_read_private_computation_instance
        )
        self.repo.delete(instance_id)

    def test_update(self):
        instance_id = self._get_random_id()
        test_update_private_computation_instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=PrivateComputationRole.PUBLISHER,
            instances=[self.test_mpc_instance],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            num_files_per_mpc_container=40,
            game_type=PrivateComputationGameType.LIFT,
        )
        # Create a new MPC instance to be added to instances
        self.repo.create(test_update_private_computation_instance)
        test_mpc_instance_new = PCSMPCInstance.create_instance(
            instance_id=instance_id,
            game_name="aggregation",
            mpc_party=MPCParty.SERVER,
            num_workers=1,
        )
        instances_new = [self.test_mpc_instance, test_mpc_instance_new]
        # Update instances
        test_update_private_computation_instance.instances = instances_new
        self.repo.update(test_update_private_computation_instance)
        # Assert instances is updated
        self.assertEqual(self.repo.read(instance_id).instances, instances_new)
        self.repo.delete(instance_id)

    def _get_random_id(self) -> str:
        return "id" + "".join(random.choice(string.ascii_letters) for i in range(10))
