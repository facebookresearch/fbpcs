#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass
from typing import List

from fbpcs.bolt.bolt_job_summary import BoltJobSummary


@dataclass
class BoltSummary:
    job_summaries: List[BoltJobSummary]

    def __bool__(self) -> bool:
        return self.is_success

    @property
    def is_success(self) -> bool:
        return self.num_failures == 0

    @property
    def is_failure(self) -> bool:
        return not self.is_success

    @property
    def num_jobs(self) -> int:
        return len(self.job_summaries)

    @property
    def num_successes(self) -> int:
        return self.num_jobs - self.num_failures

    @property
    def num_failures(self) -> int:
        return len(self.failed_job_summaries)

    @property
    def failed_job_summaries(self) -> List[BoltJobSummary]:
        return [s for s in self.job_summaries if not s.is_success]

    @property
    def failed_job_names(self) -> List[str]:
        return [s.job_name for s in self.failed_job_summaries]
