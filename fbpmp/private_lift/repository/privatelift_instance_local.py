#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.repository.instance_local import LocalInstanceRepository
from fbpmp.private_lift.entity.privatelift_instance import PrivateLiftInstance
from fbpmp.private_lift.repository.privatelift_instance import (
    PrivateLiftInstanceRepository,
)


class LocalPrivateLiftInstanceRepository(PrivateLiftInstanceRepository):
    def __init__(self, base_dir: str) -> None:
        self.repo = LocalInstanceRepository(base_dir)

    def create(self, instance: PrivateLiftInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PrivateLiftInstance:
        return PrivateLiftInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PrivateLiftInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
