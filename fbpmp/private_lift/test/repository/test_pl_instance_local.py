#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import random
import string
import unittest

from fbpcs.entity.mpc_instance import MPCInstance, MPCRole
from fbpmp.private_lift.entity.privatelift_instance import (
    PrivateLiftInstance,
    PrivateLiftInstanceStatus,
    PrivateLiftRole,
)
from fbpmp.private_lift.repository.privatelift_instance_local import (
    LocalPrivateLiftInstanceRepository,
)


class TestLocalPLInstanceRepository(unittest.TestCase):
    def setUp(self):
        instance_id = self._get_random_id()
        self.repo = LocalPrivateLiftInstanceRepository("./")
        self.test_mpc_instance = MPCInstance.create_instance(
            instance_id=instance_id,
            game_name="conversion_lift",
            mpc_role=MPCRole.SERVER,
            num_workers=2,
        )

    def test_read(self):
        instance_id = self._get_random_id()
        test_read_pl_instance = PrivateLiftInstance(
            instance_id=instance_id,
            role=PrivateLiftRole.PUBLISHER,
            instances=[self.test_mpc_instance],
            status=PrivateLiftInstanceStatus.CREATED,
            status_update_ts=1600000000,
        )
        self.repo.create(test_read_pl_instance)
        self.assertEqual(self.repo.read(instance_id), test_read_pl_instance)
        self.repo.delete(instance_id)

    def test_update(self):
        instance_id = self._get_random_id()
        test_update_pl_instance = PrivateLiftInstance(
            instance_id=instance_id,
            role=PrivateLiftRole.PUBLISHER,
            instances=[self.test_mpc_instance],
            status=PrivateLiftInstanceStatus.CREATED,
            status_update_ts=1600000000,
        )
        # Create a new MPC instance to be added to instances
        self.repo.create(test_update_pl_instance)
        test_mpc_instance_new = MPCInstance.create_instance(
            instance_id=instance_id,
            game_name="aggregation",
            mpc_role=MPCRole.SERVER,
            num_workers=1,
        )
        instances_new = [self.test_mpc_instance, test_mpc_instance_new]
        # Update instances
        test_update_pl_instance.instances = instances_new
        self.repo.update(test_update_pl_instance)
        # Assert instances is updated
        self.assertEqual(self.repo.read(instance_id).instances, instances_new)
        self.repo.delete(instance_id)

    def _get_random_id(self) -> str:
        return "id" + "".join(random.choice(string.ascii_letters) for i in range(10))
