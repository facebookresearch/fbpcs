#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.mpc_instance import (
    MPCInstance,
    MPCParty,
    MPCInstanceStatus,
)
from fbpcs.common.entity.instance_base import InstanceBase


class PCSMPCInstance(MPCInstance, InstanceBase):
    @classmethod
    def create_instance(
        cls,
        instance_id: str,
        game_name: str,
        mpc_party: MPCParty,
        num_workers: int,
        server_ips: Optional[List[str]] = None,
        containers: Optional[List[ContainerInstance]] = None,
        status: MPCInstanceStatus = MPCInstanceStatus.UNKNOWN,
        game_args: Optional[List[Dict[str, Any]]] = None,
    ) -> "PCSMPCInstance":
        return cls(
            instance_id,
            game_name,
            mpc_party,
            num_workers,
            server_ips,
            containers or [],
            status,
            game_args,
        )

    @classmethod
    def from_mpc_instance(cls, mpc_instance: MPCInstance) -> "PCSMPCInstance":
        return cls(
            mpc_instance.instance_id,
            mpc_instance.game_name,
            mpc_instance.mpc_party,
            mpc_instance.num_workers,
            mpc_instance.server_ips,
            mpc_instance.containers,
            mpc_instance.status,
            mpc_instance.game_args,
        )
