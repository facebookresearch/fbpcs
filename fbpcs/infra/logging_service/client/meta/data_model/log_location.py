# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.aws_log_location import (
    AwsLogLocation,
)

# Log metadata for a study run
@dataclass_json
@dataclass
class LogLocation:
    note: Optional[str] = None
    log_url: Optional[str] = None
    aws_log_location: Optional[AwsLogLocation] = None
