#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import cast

from fbpcp.repository.instance_s3 import S3InstanceRepository
from fbpcp.service.storage_s3 import S3StorageService
from fbpmp.pid.entity.pid_instance import PIDInstance
from fbpmp.pid.repository.pid_instance import PIDInstanceRepository


class S3PIDInstanceRepository(PIDInstanceRepository):
    def __init__(self, s3_storage_svc: S3StorageService, base_dir: str) -> None:
        super().__init__()
        self.repo = S3InstanceRepository(s3_storage_svc, base_dir)

    def create(self, instance: PIDInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PIDInstance:
        return PIDInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PIDInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
