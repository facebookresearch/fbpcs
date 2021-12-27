#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.repository.mpc_instance import MPCInstanceRepository
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.repository.instance_local import LocalInstanceRepository


class LocalMPCInstanceRepository(MPCInstanceRepository):
    def __init__(self, base_dir: str) -> None:
        self.repo = LocalInstanceRepository(base_dir)

    def create(self, instance: MPCInstance) -> None:
        self.repo.create(PCSMPCInstance.from_mpc_instance(instance))

    def read(self, instance_id: str) -> PCSMPCInstance:
        return PCSMPCInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: MPCInstance) -> None:
        self.repo.update(PCSMPCInstance.from_mpc_instance(instance))

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
