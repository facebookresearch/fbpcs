#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import cast

from fbpcs.repository.instance_local import LocalInstanceRepository
from fbpmp.private_attribution.entity.private_attribution_instance import (
    PrivateAttributionInstance,
)
from fbpmp.private_attribution.repository.private_attribution_instance import (
    PrivateAttributionInstanceRepository,
)


class LocalPrivateAttributionInstanceRepository(PrivateAttributionInstanceRepository):
    def __init__(self, base_dir: str) -> None:
        self.repo = LocalInstanceRepository(base_dir)

    def create(self, instance: PrivateAttributionInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PrivateAttributionInstance:
        return PrivateAttributionInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PrivateAttributionInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
