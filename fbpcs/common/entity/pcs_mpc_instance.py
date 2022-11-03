#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.pcs_container_instance import PCSContainerInstance


@dataclass
class PCSMPCInstance(MPCInstance, InstanceBase):
    instance_id: str
    game_name: str
    mpc_party: MPCParty
    num_workers: int
    server_ips: Optional[List[str]]  # used to establish vpc peering between containers
    containers: List[Union[PCSContainerInstance, ContainerInstance]]
    status: MPCInstanceStatus
    game_args: Optional[List[Dict[str, Any]]]
    server_uris: Optional[
        List[str]
    ]  # used to establish TLS connection between containers

    @classmethod
    def create_instance(
        cls,
        instance_id: str,
        game_name: str,
        mpc_party: MPCParty,
        num_workers: int,
        server_ips: Optional[List[str]] = None,
        containers: Optional[
            List[Union[PCSContainerInstance, ContainerInstance]]
        ] = None,
        status: MPCInstanceStatus = MPCInstanceStatus.UNKNOWN,
        game_args: Optional[List[Dict[str, Any]]] = None,
        server_uris: Optional[List[str]] = None,
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
            server_uris,
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
            mpc_instance.server_uris,
        )
