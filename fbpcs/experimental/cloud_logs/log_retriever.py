#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class LogRetriever:
    """Retrieves logs for containers under a specific cloud provider.

    Private attributes:
        _cloud_provider: Cloud Provider for which this log retriever was initialized
    """

    def __init__(self, cloud_provider: CloudProvider) -> None:
        self._cloud_provider = cloud_provider

    def get_log_url(self, container_id: str) -> str:
        """Get the log url for a container

        Args:
            container_id: identifier for container for which the log URL should be retrieved

        Returns:
            return: The log URL for said container

        Raises:
            IndexError: can be raised when using CloudProvider.AWS
            NotImplementedError: if anything other than CloudProvider.AWS is used
        """
        if self._cloud_provider is CloudProvider.AWS:
            return self._get_aws_cloudwatch_log_url(container_id)
        else:
            raise NotImplementedError(
                f"Retrieving log URLs for {self._cloud_provider} is not yet supported."
            )

    def _get_aws_cloudwatch_log_url(self, container_id: str) -> str:
        """Return a CloudWatch URL given a container id.

        Args:
            container_id: AWS arn of a container run in ECS

        Returns:
            return: The Cloudwatch URL for said container

        Raises:
            IndexError: if container_id is not well-formed
        """
        container_id_info = container_id.split(":")
        log_region = container_id_info[3]
        task_id_info = container_id_info[-1].split("/")
        cluster_name = task_id_info[1]
        container_name = cluster_name.replace("-cluster", "-container")
        task_id = task_id_info[-1]
        log_group_name = f"$252Fecs$252F{container_name}"
        log_stream_name = f"ecs$252F{container_name}$252F{task_id}"

        return (
            f"https://{log_region}.console.aws.amazon.com/cloudwatch/home?"
            f"region={log_region}#logsV2:log-groups/"
            f"log-group/{log_group_name}/"
            f"log-events/{log_stream_name}"
        )
