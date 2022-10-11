#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from dataclasses import dataclass
from enum import auto, Enum
from typing import Any, Dict, List, Optional, Type, TypedDict, Union

from fbpcp.entity.log_event import LogEvent
from fbpcp.service.log_cloudwatch import CloudWatchLogService
from fbpcp.util import reflect
from fbpcs.experimental.cloud_logs.log_retriever import LogRetriever


class CloudWatchLogServiceArgs(TypedDict):
    kls: Union[str, Type[CloudWatchLogService]]
    args: Dict[str, Any]


class LogGroupGuessStrategy(Enum):
    FROM_PCE_SERVICE = auto()
    FROM_ARN = auto()


@dataclass
class ContainerInfo:
    awslogs_group: str
    awslogs_stream: str
    awslogs_region: str


class AWSLogRetriever(LogRetriever):
    def __init__(
        self,
        awslogs_stream_prefix: str = "ecs",
        awslogs_group: Optional[str] = None,
        awslogs_region: Optional[str] = None,
        # from arn is the default because partner PCEs seem to have use it, and
        # it's easy for us to change the guess strategy for the publisher
        log_group_guess_strategy: Enum = LogGroupGuessStrategy.FROM_ARN,
        cloudwatch_log_service_args: Optional[CloudWatchLogServiceArgs] = None,
    ) -> None:
        self.awslogs_stream_prefix = awslogs_stream_prefix
        self.awslogs_group = awslogs_group
        self.awslogs_region = awslogs_region
        self.log_group_guess_strategy = log_group_guess_strategy

        self.cloudwatch_log_service_args = cloudwatch_log_service_args
        self._cloudwatch_log_svc: Optional[CloudWatchLogService] = None

        self.logger: logging.Logger = logging.getLogger(__name__)

    def get_log_url(self, container_id: str) -> str:
        """Get the log url for a container

        Args:
            container_id: ARN identifier for container for which the log URL should be retrieved

        Returns:
            return: The log URL for said container

        Raises
            IndexError: if container_id is not well-formed
        """

        container_info = self._get_container_info(container_id)

        return (
            f"https://{container_info.awslogs_region}.console.aws.amazon.com/cloudwatch/home?"
            f"region={container_info.awslogs_region}#logsV2:log-groups/"
            f"log-group/{self._aws_encode(container_info.awslogs_group)}/"
            f"log-events/{self._aws_encode(container_info.awslogs_stream)}"
        )

    def fetch(self, container_id: str) -> List[LogEvent]:
        """Get the log events for a container

        Args:
            container_id: ARN identifier for container for which the log URL should be retrieved

        Returns:
            return: A list of each log event (timestamp, msg) for the container

        Raises
            IndexError: if container_id is not well-formed
        """
        container_info = self._get_container_info(container_id)
        cloudwatch_log_svc = self._get_cloudwatch_log_svc(
            container_info.awslogs_group, container_info.awslogs_region
        )
        if cloudwatch_log_svc:
            return cloudwatch_log_svc.fetch(container_info.awslogs_stream)
        else:
            return []

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

    def _get_container_info(self, container_id: str) -> ContainerInfo:
        container_id_info = container_id.split(":")
        awslogs_region = self.awslogs_region or container_id_info[3]
        cluster_name = container_id_info[-1].split("/")[1]

        awslogs_group = self._get_log_group_name(awslogs_region, cluster_name)
        log_stream_name = self._get_log_stream_name(container_id, awslogs_group)
        return ContainerInfo(
            awslogs_group=awslogs_group,
            awslogs_stream=log_stream_name,
            awslogs_region=awslogs_region,
        )

    def _get_cloudwatch_log_svc(
        self,
        log_group_name: str,
        region: str,
    ) -> Optional[CloudWatchLogService]:
        if self._cloudwatch_log_svc:
            return self._cloudwatch_log_svc
        cloudwatch_log_service_args = self.cloudwatch_log_service_args
        if not cloudwatch_log_service_args:
            return None

        try:
            args = {
                **{"region": region, "log_group": log_group_name},
                **cloudwatch_log_service_args["args"],
            }
            kls = cloudwatch_log_service_args["kls"]
            if isinstance(kls, str):
                kls = reflect.get_class(kls)

            return kls(**args)
        except (ImportError, AttributeError) as e:
            self.logger.warning("Could not import cloudwatch log service class")
            self.logger.debug(e)
        except KeyError as e:
            self.logger.warning(
                "Issue accessing cloudwatch_log_service_args - check the keys!"
            )
            self.logger.debug(e)
        except TypeError as e:
            self.logger.warning("Could not construct cloudwatch log service class")
            self.logger.debug(e)
        except Exception as e:
            self.logger.warning("Could not retrieve a cloudwatch log service")
            self.logger.debug(e)

        return None

    @classmethod
    def _aws_encode(cls, s: str) -> str:
        return s.replace("/", "$252F")
