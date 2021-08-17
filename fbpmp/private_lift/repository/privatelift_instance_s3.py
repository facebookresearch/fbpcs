#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcp.service.storage_s3 import S3StorageService
from fbpmp.common.repository.instance_s3 import S3InstanceRepository
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpmp.private_lift.repository.privatelift_instance import (
    PrivateLiftInstanceRepository,
)


class S3PrivateLiftInstanceRepository(PrivateLiftInstanceRepository):
    def __init__(self, s3_storage_svc: S3StorageService, base_dir: str) -> None:
        self.repo = S3InstanceRepository(s3_storage_svc, base_dir)

    def create(self, instance: PrivateComputationInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PrivateComputationInstance:
        return PrivateComputationInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PrivateComputationInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
