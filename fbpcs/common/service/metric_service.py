#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import logging
import time
from contextlib import contextmanager
from typing import Iterator


class MetricService(abc.ABC):
    RUNTIME_KEY = "time_ms"
    NUM_ERRORS_KEY = "num_errors"
    NUM_CALLS_KEY = "num_calls"

    def __init__(self, category: str = "default") -> None:
        self.category = category
        self.logger: logging.Logger = logging.getLogger(__name__)

    @abc.abstractmethod
    def bump_entity_key(self, entity: str, key: str, value: int = 1) -> None:
        pass

    @abc.abstractmethod
    def bump_entity_key_avg(self, entity: str, key: str, value: int = 1) -> None:
        pass

    @contextmanager
    def timer(self, entity: str, prefix: str) -> Iterator[None]:
        """
        Log code execution time in ms
        """
        start_ns = time.perf_counter_ns()
        yield
        elapsed_ns = time.perf_counter_ns() - start_ns
        self.bump_entity_key_avg(
            entity=entity,
            key=f"{prefix}.{MetricService.RUNTIME_KEY}",
            value=int(elapsed_ns / 1e6),
        )

    @contextmanager
    def bump_num_times_called_and_error_count(
        self, entity: str, prefix: str
    ) -> Iterator[None]:
        """Context manager that bumps num calls and error counts for a given prefix.

        expected usage is:

        with ods_client.bump_num_times_called_and_error_count(prefix):
            <do something>
        """
        try:
            yield
        except Exception:
            self.bump_entity_key(
                entity=entity, key=f"{prefix}.{MetricService.NUM_ERRORS_KEY}"
            )
            raise
        finally:
            self.bump_entity_key(
                entity=entity, key=f"{prefix}.{MetricService.NUM_CALLS_KEY}"
            )
