#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import random
import string
import unittest

from fbpcp.entity.mpc_instance import MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_instance_local import (
    LocalPrivateComputationInstanceRepository,
)


class TestLocalPrivateComputationInstanceRepository(unittest.TestCase):
    def setUp(self) -> None:
        instance_id = self._get_random_id()
        self.repo = LocalPrivateComputationInstanceRepository("./")
        self.test_mpc_instance = PCSMPCInstance.create_instance(
            instance_id=instance_id,
            game_name="conversion_lift",
            mpc_party=MPCParty.SERVER,
            num_workers=2,
        )

    def test_read(self) -> None:
        instance_id = self._get_random_id()
        infra_config: InfraConfig = InfraConfig(
            instance_id=instance_id,
            role=PrivateComputationRole.PUBLISHER,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            instances=[self.test_mpc_instance],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=4,
            num_mpc_containers=4,
            num_files_per_mpc_container=40,
            mpc_compute_concurrency=1,
        )
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )

        test_read_private_computation_instance = PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
            input_path="in",
            output_dir="out",
        )
        self.repo.create(test_read_private_computation_instance)
        self.assertEqual(
            self.repo.read(instance_id), test_read_private_computation_instance
        )
        self.repo.delete(instance_id)

    def test_create_with_invalid_num_containers(self) -> None:
        instance_id = self._get_random_id()
        infra_config: InfraConfig = InfraConfig(
            instance_id=instance_id,
            role=PrivateComputationRole.PUBLISHER,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            instances=[self.test_mpc_instance],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=8,
            num_mpc_containers=4,
            num_files_per_mpc_container=40,
            mpc_compute_concurrency=1,
        )
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )
        with self.assertRaises(ValueError):
            PrivateComputationInstance(
                infra_config=infra_config,
                product_config=product_config,
                input_path="in",
                output_dir="out",
            )

    def test_update(self) -> None:
        instance_id = self._get_random_id()
        infra_config: InfraConfig = InfraConfig(
            instance_id=instance_id,
            role=PrivateComputationRole.PUBLISHER,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            instances=[self.test_mpc_instance],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=4,
            num_mpc_containers=4,
            num_files_per_mpc_container=40,
            mpc_compute_concurrency=1,
        )
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )
        test_update_private_computation_instance = PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
            input_path="in",
            output_dir="out",
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
        test_update_private_computation_instance.infra_config.instances = instances_new
        self.repo.update(test_update_private_computation_instance)
        # Assert instances is updated
        self.assertEqual(
            self.repo.read(instance_id).infra_config.instances, instances_new
        )
        self.repo.delete(instance_id)

    def _get_random_id(self) -> str:
        return "id" + "".join(random.choice(string.ascii_letters) for i in range(10))
