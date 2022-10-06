#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import auto, Enum
from typing import Optional

from fbpcs.experimental.cloud_logs.log_retriever import LogRetriever


class LogGroupGuessStrategy(Enum):
    FROM_PCE_SERVICE = auto()
    FROM_ARN = auto()


class AWSLogRetriever(LogRetriever):
    def __init__(
        self,
        awslogs_stream_prefix: str = "ecs",
        awslogs_group: Optional[str] = None,
        awslogs_region: Optional[str] = None,
        # from arn is the default because partner PCEs seem to have use it, and
        # it's easy for us to change the guess strategy for the publisher
        log_group_guess_strategy: Enum = LogGroupGuessStrategy.FROM_ARN,
    ) -> None:
        self.awslogs_stream_prefix = awslogs_stream_prefix
        self.awslogs_group = awslogs_group
        self.awslogs_region = awslogs_region
        self.log_group_guess_strategy = log_group_guess_strategy

    def get_log_url(self, container_id: str) -> str:
        """Get the log url for a container

        Args:
            container_id: ARN identifier for container for which the log URL should be retrieved

        Returns:
            return: The log URL for said container

        Raises
            IndexError: if container_id is not well-formed
        """

        container_id_info = container_id.split(":")
        awslogs_region = self.awslogs_region or container_id_info[3]
        cluster_name = container_id_info[-1].split("/")[1]

        awslogs_group = self._get_log_group_name(awslogs_region, cluster_name)
        log_stream_name = self._get_log_stream_name(container_id, awslogs_group)

        return (
            f"https://{awslogs_region}.console.aws.amazon.com/cloudwatch/home?"
            f"region={awslogs_region}#logsV2:log-groups/"
            f"log-group/{self._aws_encode(awslogs_group)}/"
            f"log-events/{self._aws_encode(log_stream_name)}"
        )

    def _get_log_group_name(self, region: str, cluster: str) -> str:
        if self.awslogs_group:
            return self.awslogs_group
        elif self.log_group_guess_strategy is LogGroupGuessStrategy.FROM_PCE_SERVICE:
            return f"/ecs/onedocker-container-shared-{region}"
        elif self.log_group_guess_strategy is LogGroupGuessStrategy.FROM_ARN:
            return f"/ecs/{cluster.replace('-cluster', '-container')}"
        else:
            raise ValueError("Cannot guess log group name")

    def _get_log_stream_name(self, container_id: str, log_group_name: str) -> str:
        container_name = log_group_name.split("/")[-1]
        task_id = container_id.split("/")[-1]

        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html
        # search for awslogs-stream-prefix
        return f"{self.awslogs_stream_prefix}/{container_name}/{task_id}"

    @classmethod
    def _aws_encode(cls, s: str) -> str:
        return s.replace("/", "$252F")
