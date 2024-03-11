#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from abc import ABC, abstractmethod
from typing import List

from fbpcp.entity.log_event import LogEvent


class LogRetriever(ABC):
    """Retrieves logs for containers under a specific cloud provider."""

    @abstractmethod
    def get_log_url(self, container_id: str) -> str:
        """Get the log url for a container

        Args:
            container_id: identifier for container for which the log URL should be retrieved

        Returns:
            return: The log URL for said container
        """
        ...

    def fetch(self, container_id: str) -> List[LogEvent]:
        return []

    def log_events_to_str(self, events: List[LogEvent]) -> str:
        return "\n".join(f"{event.timestamp}: {event.message}" for event in events)
