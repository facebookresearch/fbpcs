#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.repository.instance_s3 import S3InstanceRepository
from fbpcs.service.storage_s3 import S3StorageService
from fbpmp.private_lift.entity.privatelift_instance import PrivateLiftInstance
from fbpmp.private_lift.repository.privatelift_instance import (
    PrivateLiftInstanceRepository,
)


class S3PrivateLiftInstanceRepository(PrivateLiftInstanceRepository):
    def __init__(self, s3_storage_svc: S3StorageService, base_dir: str) -> None:
        self.repo = S3InstanceRepository(s3_storage_svc, base_dir)

    def create(self, instance: PrivateLiftInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PrivateLiftInstance:
        return PrivateLiftInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PrivateLiftInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
