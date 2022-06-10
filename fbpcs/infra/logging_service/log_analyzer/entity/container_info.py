# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.log_analyzer.entity.log_context import LogContext


@dataclass_json
@dataclass
class ContainerInfo:
    context: LogContext
    container_id: str
    log_url: Optional[str] = None
    status: Optional[str] = None
