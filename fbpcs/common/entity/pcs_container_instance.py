#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json
from fbpcp.entity.container_instance import ContainerInstance


@dataclass_json
@dataclass
class PCSContainerInstance(ContainerInstance):
    log_url: Optional[str] = None

    @classmethod
    def from_container_instance(
        cls, container_instance: ContainerInstance, log_url: Optional[str] = None
    ) -> "PCSContainerInstance":
        return cls(
            instance_id=container_instance.instance_id,
            ip_address=container_instance.ip_address,
            status=container_instance.status,
            log_url=log_url,
        )
