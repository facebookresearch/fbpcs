#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Dict, List, Optional, Union

from fbpcp.entity.cluster_instance import Cluster
from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.container_permission import ContainerPermissionConfig
from fbpcp.entity.container_type import ContainerType
from fbpcp.error.pcp import PcpError
from fbpcp.service.container import ContainerService
from fbpcp.service.container_aws import AWSContainerService
from fbpcs.common.entity.pcs_container_instance import PCSContainerInstance
from fbpcs.experimental.cloud_logs.aws_log_retriever import AWSLogRetriever
from fbpcs.experimental.cloud_logs.log_retriever import LogRetriever
from fbpcs.utils.deprecated import deprecated


class PCSContainerService(ContainerService):
    def __init__(
        self,
        inner_container_service: ContainerService,
        log_retriever: Optional[LogRetriever] = None,
    ) -> None:
        self.inner_container_service: ContainerService = inner_container_service
        self.log_retriever: Optional[LogRetriever] = log_retriever
        if not self.log_retriever:
            if isinstance(self.inner_container_service, AWSContainerService):
                self.log_retriever = AWSLogRetriever()

    def get_region(
        self,
    ) -> str:
        return self.inner_container_service.get_region()

    def get_cluster(
        self,
    ) -> str:
        return self.inner_container_service.get_cluster()

    def create_instance(
        self,
        container_definition: str,
        cmd: str,
        env_vars: Optional[Dict[str, str]] = None,
        container_type: Optional[ContainerType] = None,
        permission: Optional[ContainerPermissionConfig] = None,
    ) -> ContainerInstance:
        instance = self.inner_container_service.create_instance(
            container_definition=container_definition,
            cmd=cmd,
            env_vars=env_vars,
            container_type=container_type,
            permission=permission,
        )
        log_url = None
        if self.log_retriever:
            log_url = self.log_retriever.get_log_url(instance.instance_id)

        return PCSContainerInstance.from_container_instance(instance, log_url)

    def create_instances(
        self,
        container_definition: str,
        cmds: List[str],
        env_vars: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None,
        container_type: Optional[ContainerType] = None,
        permission: Optional[ContainerPermissionConfig] = None,
    ) -> List[ContainerInstance]:
        """
        Args:
            container_definition: a string representing the container definition.
            cmds: A list of cmds per instance to run inside each instance.
            env_vars: A dictionary or a list of dictionaries of env_vars to be set in instances.
            When it is a single dictionary, all env vars in the dict will be set in all
            instances. When it is a list of dicts, it is expected that the length of the list
            is the same as the length of the cmds list, such that each item corresponds
            to one instance.
            container_type: The type of container to create.
            permission: A configuration which describes the container permissions
        Returns:
            A list of ContainerInstances.
        """
        if type(env_vars) is list and len(env_vars) != len(cmds):
            raise ValueError(
                f"Length of env_vars list {len(env_vars)} is different from length of cmds {len(cmds)}."
            )

        instances = [
            self.create_instance(
                container_definition=container_definition,
                cmd=cmds[i],
                env_vars=env_vars[i] if type(env_vars) is list else env_vars,
                container_type=container_type,
                permission=permission,
            )
            for i in range(len(cmds))
        ]

        return instances

    def _map_container_instance_to_pcs_container_instance(
        self, instance_id: str, instance: ContainerInstance
    ) -> PCSContainerInstance:
        log_url = None
        if self.log_retriever:
            log_url = self.log_retriever.get_log_url(instance.instance_id)
        return PCSContainerInstance.from_container_instance(instance, log_url)

    def get_instance(self, instance_id: str) -> Optional[ContainerInstance]:
        instance = self.inner_container_service.get_instance(instance_id)
        if instance is not None:
            return self._map_container_instance_to_pcs_container_instance(
                instance_id, instance
            )

    def get_instances(
        self, instance_ids: List[str]
    ) -> List[Optional[ContainerInstance]]:
        pcs_container_instances = []
        instances = self.inner_container_service.get_instances(instance_ids)
        for instance in instances:
            if instance:
                pcs_container_instances.append(
                    self._map_container_instance_to_pcs_container_instance(
                        instance.instance_id, instance
                    )
                )

        return pcs_container_instances

    def cancel_instance(self, instance_id: str) -> None:
        return self.inner_container_service.cancel_instance(instance_id)

    def cancel_instances(self, instance_ids: List[str]) -> List[Optional[PcpError]]:
        return self.inner_container_service.cancel_instances(instance_ids)

    def get_current_instances_count(self) -> int:
        return self.inner_container_service.get_current_instances_count()

    @deprecated(
        "validate_container_definition is no longer a public method in container service"
    )
    def validate_container_definition(self, container_definition: str) -> None:
        pass

    def get_cluster_instance(self) -> Cluster:
        raise NotImplementedError
